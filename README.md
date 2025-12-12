# runout-calc

# Run-out Kanban + Early Warning (Streamlit)

## What it does
- Upload Demand + Inventory Excel files
- Uses rules:
  - Item is the part number
  - Demand = Quantity
  - Aerostar Ship Week in format "YYYY - WkNN" (ISO week)
  - Demand due by Friday of that ISO week
  - Inventory = sum(Available) where Status=good
  - Run-out = first week where cumulative demand exceeds inventory
- Renders a scrollable 12-week Kanban
- Early Warning view (Critical/Imminent/Watch)
- Optional snapshot compare (upload previous CSV)

## Run locally
```bash
pip install -r requirements.txt
streamlit run app.py
