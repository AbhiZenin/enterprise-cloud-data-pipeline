import pandas as pd
from src.enterprise_pipeline.scd import apply_scd_type2

def test_scd_type2_creates_new_version():
    current = pd.DataFrame([{
        "customer_id":"C1","customer_name":"A","email":"a@x.com","state":"TX",
        "signup_date":"2025-01-01","effective_from":"t1","effective_to":None,"is_current":True
    }])
    incoming = pd.DataFrame([{
        "customer_id":"C1","customer_name":"A","email":"a@x.com","state":"CA",
        "signup_date":"2025-01-01"
    }])
    result = apply_scd_type2(current, incoming, "customer_id", ["state"], "t2")
    assert len(result) == 2
    assert result["is_current"].sum() == 1
    assert result[result["is_current"] == True].iloc[0]["state"] == "CA"
