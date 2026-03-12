from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
import psycopg2
import os
from supabase import create_client, Client

app = FastAPI(
    title="Crowdsourced Nutrition API",
    description="MVP API for foods, variants, nutrients, and contributions",
    version="0.2"
)

# ==========================
# Database connection helper
# ==========================
from dotenv import load_dotenv
load_dotenv(".env.local")

DB_URL = os.environ.get("DATABASE_URL") 
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def get_connection():
    return psycopg2.connect(os.environ["DATABASE_URL"], sslmode="require")

# ==========================
# Models
# ==========================
class FoodCreate(BaseModel):
    name: str
    category: str = None
    is_packaged: bool = False
    barcode: str = None

class VariantCreate(BaseModel):
    food_id: int
    variant_label: str
    serving_size: str = "100g"
    notes: str = None
    nutrients: dict = None  # e.g., {"calories":165, "protein":25,...}

class ContributionCreate(BaseModel):
    variant_id: int
    field_name: str
    value: float

class VariantCreateNoFoodID(BaseModel):
    variant_label: str
    serving_size: str = "100g"
    notes: str = None
    nutrients: dict = None


class FoodWithVariant(BaseModel):
    food: FoodCreate
    variant: VariantCreateNoFoodID

# ==========================
# Endpoints
# ==========================

@app.get("/foods/query")
def search_foods(
    q: str = Query(..., description="Search query like 'chicken boiled'"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0)
):
    """
    Search foods and variants from the crowdsourced nutrition database.
    """
    try:
        # Call the RPC function
        result = supabase.rpc(
            "foods_search",
            {"q": q, "limit_count": limit, "offset_count": offset}
        )

        # `.execute()` returns SingleAPIResponse; `.data` contains the actual result
        return result.execute().data

    except Exception as e:
        # If the RPC fails, raise 500 with the error message
        raise HTTPException(status_code=500, detail=str(e))

# 2. Add new food
@app.post("/foods/add")
def add_food_with_variant(payload: FoodWithVariant):
    food = payload.food
    variant = payload.variant

    conn = get_connection()
    cur = conn.cursor()

    # Check if food exists
    cur.execute("SELECT food_id FROM Foods WHERE LOWER(name) = LOWER(%s)", (food.name,))
    existing = cur.fetchone()
    if existing:
        cur.close()
        conn.close()
        raise HTTPException(status_code=400, detail=f"Food already exists with id {existing[0]}")

    try:
        # Insert food
        cur.execute(
            """
            INSERT INTO Foods (name, category, is_packaged, barcode)
            VALUES (%s, %s, %s, %s)
            RETURNING food_id
            """,
            (food.name, food.category, food.is_packaged, food.barcode)
        )
        food_id = cur.fetchone()[0]

        # Insert variant
        cur.execute(
            """
            INSERT INTO Food_Variants (food_id, variant_label, serving_size, notes)
            VALUES (%s, %s, %s, %s)
            RETURNING variant_id
            """,
            (food_id, variant.variant_label, variant.serving_size, variant.notes)
        )
        variant_id = cur.fetchone()[0]

        # Insert nutrients if provided
        if variant.nutrients:
            fields = ','.join(variant.nutrients.keys())
            placeholders = ','.join(['%s']*len(variant.nutrients))
            values = list(variant.nutrients.values())
            query = f"""
                INSERT INTO Food_Nutrients (variant_id, {fields}, confidence_score)
                VALUES (%s, {placeholders}, 0.5)
            """
            cur.execute(query, [variant_id] + values)

        conn.commit()
        return {"status": "success", "food_id": food_id, "variant_id": variant_id}

    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        cur.close()
        conn.close()

# 3. Add new variant
@app.post("/variants/add")
def add_variant(variant: VariantCreate):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO Food_Variants (food_id, variant_label, serving_size, notes)
        VALUES (%s,%s,%s,%s)
        RETURNING variant_id
        """,
        (variant.food_id, variant.variant_label, variant.serving_size, variant.notes)
    )
    variant_id = cur.fetchone()[0]

    # Insert nutrients if provided
    if variant.nutrients:
        fields = ','.join(variant.nutrients.keys())
        placeholders = ','.join(['%s']*len(variant.nutrients))
        values = list(variant.nutrients.values())
        query = f"""
            INSERT INTO Food_Nutrients (variant_id, {fields}, confidence_score)
            VALUES (%s, {placeholders}, 0.5)
        """
        cur.execute(query, [variant_id]+values)

    conn.commit()
    cur.close()
    conn.close()
    return {"status": "success", "variant_id": variant_id}

# 4. Add contribution
@app.post("/contributions")
def add_contribution(contrib: ContributionCreate):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO Contribution_Summary (variant_id, field_name, value)
            VALUES (%s,%s,%s)
            ON CONFLICT (variant_id, field_name, value)
            DO UPDATE SET contribution_count = Contribution_Summary.contribution_count + 1,
                          last_contributed = NOW()
            RETURNING contribution_id
            """,
            (contrib.variant_id, contrib.field_name, contrib.value)
        )
        contrib_id = cur.fetchone()[0]
        conn.commit()
    except Exception as e:
        conn.rollback()
        cur.close()
        conn.close()
        raise HTTPException(status_code=400, detail=str(e))
    cur.close()
    conn.close()
    return {"status": "success", "contribution_id": contrib_id}

# 5. List variants for a food
@app.get("/foods/{food_id}/variants")
def list_variants(food_id: int):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT fv.variant_id, fv.variant_label, fv.serving_size, fv.notes,
               fn.calories, fn.protein, fn.carbs, fn.fat, fn.fiber, fn.sugar, fn.sodium
        FROM Food_Variants fv
        LEFT JOIN Food_Nutrients fn ON fv.variant_id = fn.variant_id
        WHERE fv.food_id = %s
        ORDER BY fv.variant_label
        """,
        (food_id,)
    )
    rows = cur.fetchall()
    variants = []
    for r in rows:
        variants.append({
            "variant_id": r[0],
            "variant_label": r[1],
            "serving_size": r[2],
            "notes": r[3],
            "nutrients": {
                "calories": r[4],
                "protein": r[5],
                "carbs": r[6],
                "fat": r[7],
                "fiber": r[8],
                "sugar": r[9],
                "sodium": r[10]
            }
        })
    cur.close()
    conn.close()
    return {"food_id": food_id, "variants": variants}

# 6. Refresh materialized view
@app.post("/refresh-view")
def refresh_materialized_view():
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("REFRESH MATERIALIZED VIEW Food_Search_View")
        conn.commit()
    except Exception as e:
        conn.rollback()
        cur.close()
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))
    cur.close()
    conn.close()
    return {"status": "success", "message": "Materialized view refreshed"}
