# Store Profit Dashboard

Internal Streamlit dashboard for:
- Shopify orders
- CJ COGS
- Meta ad spend
- CSV order-level overrides
- Default SKU cost fallback

## Run

1. Create a virtualenv.
2. Install requirements.
3. Copy `.env.example` to `.env` and fill your credentials.
4. Start with:

```bash
streamlit run app.py
```

## CSV override format

Headers supported:
- order_name or order_number
- sku (optional)
- quantity
- unit_cogs
- shipping_cost
- extra_cost
- notes

## Default SKU cost CSV

Headers:
- sku
- unit_cogs
- extra_cost (optional)

## Matching logic

1. CSV order override
2. CJ order match by normalized order number
3. Default SKU cost
4. Missing cost flag

## Notes

- Meta spend is allocated across each day’s orders by that order’s share of the day’s net sales.
- Shopify payment fees are configurable in the sidebar.
- If your CJ order number does not map cleanly to Shopify order name/number, add CSV overrides or adjust the matcher.
