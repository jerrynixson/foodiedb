import streamlit as st
import pandas as pd
import os
import psycopg2
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv(".env.local")
load_dotenv(".env")

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
DATABASE_URL = os.environ.get("DATABASE_URL")

ALLOWED_NUTRIENT_FIELDS = {
    "calories", "protein", "carbs", "fat", "fiber", "sugar", "sodium"
}

@st.cache_resource
def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def get_connection():
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def flatten_search_results(data):
    """
    The foods_search RPC returns grouped JSONB:
      [ { food_id, food_name, category, variants: [ {variant_id, variant_label, notes, serving_size, nutrients, confidence}, ... ] }, ... ]
    Flatten into one row per variant for display.
    """
    rows = []
    for food in data:
        variants = food.get("variants", [])
        for v in variants:
            nutrients = v.get("nutrients") or {}
            rows.append({
                "food_id": food.get("food_id"),
                "food_name": food.get("food_name"),
                "category": food.get("category"),
                "variant_id": v.get("variant_id"),
                "variant_label": v.get("variant_label"),
                "serving_size": v.get("serving_size"),
                "notes": v.get("notes"),
                "calories": nutrients.get("calories"),
                "protein": nutrients.get("protein"),
                "carbs": nutrients.get("carbs"),
                "fat": nutrients.get("fat"),
                "fiber": nutrients.get("fiber"),
                "sugar": nutrients.get("sugar"),
                "sodium": nutrients.get("sodium"),
                "confidence": v.get("confidence"),
            })
    return rows


# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="OpenFood DB",
    page_icon="🥗",
    layout="wide",
)

st.title("🥗 OpenFood Nutrition Database")

tab_search, tab_add_food, tab_add_variant, tab_contribute, tab_view_variants, tab_admin = st.tabs([
    "🔍 Search Foods",
    "➕ Add Food",
    "🍽️ Add Variant",
    "✏️ Contribute Data",
    "📋 View Variants",
    "⚙️ Admin",
])


# ── TAB 1 · Search ─────────────────────────────────────────────────────────────
with tab_search:
    st.header("Search Foods")

    with st.form("search_form"):
        query = st.text_input("Search query", placeholder="e.g. chicken boiled")
        col1, col2 = st.columns(2)
        limit = col1.number_input("Limit", min_value=1, max_value=100, value=20)
        offset = col2.number_input("Offset", min_value=0, value=0)
        submitted = st.form_submit_button("Search", use_container_width=True)

    if submitted and query.strip():
        try:
            supabase = get_supabase()
            result = supabase.rpc(
                "foods_search",
                {"q": query.strip(), "limit_count": int(limit), "offset_count": int(offset)}
            ).execute()
            data = result.data
            if data:
                flat = flatten_search_results(data)
                if flat:
                    st.success(f"Found {len(flat)} variant(s) across {len(data)} food(s)")
                    df = pd.DataFrame(flat)
                    st.dataframe(df, use_container_width=True, hide_index=True)
                else:
                    st.info("No variants found in results.")
            else:
                st.info("No results found.")
        except Exception as e:
            st.error(f"Search failed: {e}")
    elif submitted:
        st.warning("Please enter a search query.")


# ── TAB 2 · Add Food + Variant ─────────────────────────────────────────────────
with tab_add_food:
    st.header("Add New Food with Variant")

    with st.form("add_food_form"):
        st.subheader("Food Details")
        food_name = st.text_input("Food Name *", placeholder="e.g. Chicken Breast")
        food_category = st.text_input("Category", placeholder="e.g. Poultry")
        col_pkg, col_barcode = st.columns(2)
        is_packaged = col_pkg.checkbox("Packaged Product")
        barcode = col_barcode.text_input("Barcode", placeholder="Optional")

        st.divider()
        st.subheader("Variant Details")
        variant_label = st.text_input("Variant Label *", placeholder="e.g. Boiled, 100g")
        serving_size = st.text_input("Serving Size", value="100g")
        notes = st.text_area("Notes", placeholder="Optional notes about this variant")

        st.divider()
        st.subheader("Nutrients (optional)")
        n_col1, n_col2, n_col3 = st.columns(3)
        calories = n_col1.number_input("Calories (kcal)", min_value=0.0, value=0.0)
        protein  = n_col2.number_input("Protein (g)",    min_value=0.0, value=0.0)
        carbs    = n_col3.number_input("Carbs (g)",      min_value=0.0, value=0.0)
        fat      = n_col1.number_input("Fat (g)",        min_value=0.0, value=0.0)
        fiber    = n_col2.number_input("Fiber (g)",      min_value=0.0, value=0.0)
        sugar    = n_col3.number_input("Sugar (g)",      min_value=0.0, value=0.0)
        sodium   = n_col1.number_input("Sodium (mg)",    min_value=0.0, value=0.0)

        submitted = st.form_submit_button("Add Food", use_container_width=True, type="primary")

    if submitted:
        if not food_name.strip() or not variant_label.strip():
            st.error("Food Name and Variant Label are required.")
        else:
            nutrients = {
                k: v for k, v in {
                    "calories": calories, "protein": protein, "carbs": carbs,
                    "fat": fat, "fiber": fiber, "sugar": sugar, "sodium": sodium
                }.items() if v > 0
            }
            try:
                conn = get_connection()
                cur = conn.cursor()
                cur.execute(
                    "SELECT food_id FROM Foods WHERE LOWER(name) = LOWER(%s)",
                    (food_name.strip(),)
                )
                existing = cur.fetchone()
                if existing:
                    st.error(f"Food already exists with ID {existing[0]}.")
                    cur.close(); conn.close()
                else:
                    cur.execute(
                        """
                        INSERT INTO Foods (name, category, is_packaged, barcode)
                        VALUES (%s, %s, %s, %s)
                        RETURNING food_id
                        """,
                        (food_name.strip(), food_category.strip() or None,
                         is_packaged, barcode.strip() or None)
                    )
                    food_id = cur.fetchone()[0]

                    cur.execute(
                        """
                        INSERT INTO Food_Variants (food_id, variant_label, serving_size, notes)
                        VALUES (%s, %s, %s, %s)
                        RETURNING variant_id
                        """,
                        (food_id, variant_label.strip(),
                         serving_size.strip() or "100g",
                         notes.strip() or None)
                    )
                    variant_id = cur.fetchone()[0]

                    if nutrients:
                        # Only allow known nutrient columns to prevent injection
                        safe_fields = {k: v for k, v in nutrients.items() if k in ALLOWED_NUTRIENT_FIELDS}
                        if safe_fields:
                            cols = ", ".join(safe_fields.keys())
                            placeholders = ", ".join(["%s"] * len(safe_fields))
                            cur.execute(
                                f"INSERT INTO Food_Nutrients (variant_id, {cols}, confidence_score) "
                                f"VALUES (%s, {placeholders}, 0.5)",
                                [variant_id] + list(safe_fields.values())
                            )

                    conn.commit()
                    cur.close(); conn.close()
                    st.success(f"✅ Food added! food_id={food_id}, variant_id={variant_id}")

            except Exception as e:
                st.error(f"Error: {e}")


# ── TAB 3 · Add Variant ────────────────────────────────────────────────────────
with tab_add_variant:
    st.header("Add Variant to Existing Food")

    with st.form("add_variant_form"):
        food_id_input = st.number_input("Food ID *", min_value=1, step=1)
        variant_label = st.text_input("Variant Label *", placeholder="e.g. Grilled, 100g")
        serving_size = st.text_input("Serving Size", value="100g")
        notes = st.text_area("Notes", placeholder="Optional")

        st.divider()
        st.subheader("Nutrients (optional)")
        n_col1, n_col2, n_col3 = st.columns(3)
        calories = n_col1.number_input("Calories (kcal)", min_value=0.0, value=0.0, key="v_cal")
        protein  = n_col2.number_input("Protein (g)",    min_value=0.0, value=0.0, key="v_pro")
        carbs    = n_col3.number_input("Carbs (g)",      min_value=0.0, value=0.0, key="v_carbs")
        fat      = n_col1.number_input("Fat (g)",        min_value=0.0, value=0.0, key="v_fat")
        fiber    = n_col2.number_input("Fiber (g)",      min_value=0.0, value=0.0, key="v_fiber")
        sugar    = n_col3.number_input("Sugar (g)",      min_value=0.0, value=0.0, key="v_sugar")
        sodium   = n_col1.number_input("Sodium (mg)",    min_value=0.0, value=0.0, key="v_sodium")

        submitted = st.form_submit_button("Add Variant", use_container_width=True, type="primary")

    if submitted:
        if not variant_label.strip():
            st.error("Variant Label is required.")
        else:
            nutrients = {
                k: v for k, v in {
                    "calories": calories, "protein": protein, "carbs": carbs,
                    "fat": fat, "fiber": fiber, "sugar": sugar, "sodium": sodium
                }.items() if v > 0
            }
            try:
                conn = get_connection()
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT INTO Food_Variants (food_id, variant_label, serving_size, notes)
                    VALUES (%s, %s, %s, %s)
                    RETURNING variant_id
                    """,
                    (int(food_id_input), variant_label.strip(),
                     serving_size.strip() or "100g",
                     notes.strip() or None)
                )
                variant_id = cur.fetchone()[0]

                if nutrients:
                    safe_fields = {k: v for k, v in nutrients.items() if k in ALLOWED_NUTRIENT_FIELDS}
                    if safe_fields:
                        cols = ", ".join(safe_fields.keys())
                        placeholders = ", ".join(["%s"] * len(safe_fields))
                        cur.execute(
                            f"INSERT INTO Food_Nutrients (variant_id, {cols}, confidence_score) "
                            f"VALUES (%s, {placeholders}, 0.5)",
                            [variant_id] + list(safe_fields.values())
                        )

                conn.commit()
                cur.close(); conn.close()
                st.success(f"✅ Variant added! variant_id={variant_id}")
            except Exception as e:
                st.error(f"Error: {e}")


# ── TAB 4 · Contribute ─────────────────────────────────────────────────────────
with tab_contribute:
    st.header("Contribute Nutrition Data")
    st.caption("Submit a data point for an existing variant. Repeated contributions increase confidence.")

    with st.form("contribution_form"):
        variant_id_input = st.number_input("Variant ID *", min_value=1, step=1)
        field_name = st.selectbox(
            "Nutrient Field *",
            ["calories", "protein", "carbs", "fat", "fiber", "sugar", "sodium"]
        )
        value = st.number_input("Value *", min_value=0.0, step=0.1)
        submitted = st.form_submit_button("Submit Contribution", use_container_width=True, type="primary")

    if submitted:
        try:
            conn = get_connection()
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO Contribution_Summary (variant_id, field_name, value)
                VALUES (%s, %s, %s)
                ON CONFLICT (variant_id, field_name, value)
                DO UPDATE SET contribution_count = Contribution_Summary.contribution_count + 1,
                              last_contributed = NOW()
                RETURNING contribution_id
                """,
                (int(variant_id_input), field_name, float(value))
            )
            contrib_id = cur.fetchone()[0]
            conn.commit()
            cur.close(); conn.close()
            st.success(f"✅ Contribution recorded! contribution_id={contrib_id}")
        except Exception as e:
            st.error(f"Error: {e}")


# ── TAB 5 · View Variants ──────────────────────────────────────────────────────
with tab_view_variants:
    st.header("View Variants for a Food")

    col_search, col_id = st.columns([2, 1])
    with col_search:
        food_search_q = st.text_input("Search food by name", placeholder="e.g. chicken", key="food_lookup")
    with col_id:
        food_id_view = st.number_input("Or enter Food ID directly", min_value=1, step=1, key="view_fid")

    # Quick food name lookup
    if food_search_q and food_search_q.strip():
        try:
            conn = get_connection()
            cur = conn.cursor()
            cur.execute(
                "SELECT food_id, name, category FROM foods WHERE name ILIKE %s LIMIT 10",
                (f"%{food_search_q.strip()}%",)
            )
            matches = cur.fetchall()
            cur.close(); conn.close()
            if matches:
                st.caption("Matching foods:")
                for m in matches:
                    st.write(f"**ID {m[0]}** — {m[1]} ({m[2] or 'no category'})")
            else:
                st.info("No matching foods found.")
        except Exception as e:
            st.error(f"Lookup error: {e}")

    if st.button("Load Variants", use_container_width=True, key="load_variants_btn"):
        try:
            conn = get_connection()
            cur = conn.cursor()
            cur.execute(
                """
                SELECT fv.variant_id, fv.variant_label, fv.serving_size, fv.notes,
                       fn.calories, fn.protein, fn.carbs, fn.fat,
                       fn.fiber, fn.sugar, fn.sodium, fn.confidence_score
                FROM food_variants fv
                LEFT JOIN food_nutrients fn ON fv.variant_id = fn.variant_id
                WHERE fv.food_id = %s
                ORDER BY fv.variant_label
                """,
                (int(food_id_view),)
            )
            rows = cur.fetchall()
            cur.close(); conn.close()

            if rows:
                data = [
                    {
                        "variant_id": r[0], "label": r[1], "serving_size": r[2],
                        "notes": r[3], "calories": r[4], "protein": r[5],
                        "carbs": r[6], "fat": r[7], "fiber": r[8],
                        "sugar": r[9], "sodium": r[10], "confidence": r[11]
                    }
                    for r in rows
                ]
                st.success(f"Found {len(data)} variant(s) for food_id={int(food_id_view)}")
                st.dataframe(pd.DataFrame(data), use_container_width=True, hide_index=True)
            else:
                st.info("No variants found for this food ID.")
        except Exception as e:
            st.error(f"Error: {e}")


# ── TAB 6 · Admin ──────────────────────────────────────────────────────────────
with tab_admin:
    st.header("Admin")
    st.subheader("Refresh Materialized View")
    st.caption("Rebuilds the `Food_Search_View` used by the search endpoint.")

    if st.button("🔄 Refresh Search View", type="primary"):
        try:
            conn = get_connection()
            cur = conn.cursor()
            cur.execute("REFRESH MATERIALIZED VIEW Food_Search_View")
            conn.commit()
            cur.close(); conn.close()
            st.success("✅ Materialized view refreshed successfully.")
        except Exception as e:
            st.error(f"Error: {e}")
