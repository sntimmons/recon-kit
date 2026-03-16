from __future__ import annotations

from connectors.adp_vantage import ADPVantage
from connectors.adp_workforce_now import ADPWorkforceNow
from connectors.workday import Workday

CONNECTORS = {
    "ADP Workforce Now": ADPWorkforceNow,
    "ADP Vantage HCM": ADPVantage,
    "Workday": Workday,
    "Generic (manual mapping)": None,
}


def get_connector(name: str):
    cls = CONNECTORS.get(name)
    return cls() if cls else None
