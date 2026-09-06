import os

LANDING_ROOT = os.getenv(
  "LANDING_ROOT",
  "/opt/spark/data/landing",
)

LAKEHOUSE_ROOT = os.getenv(
  "LAKEHOUSE_ROOT",
  "/opt/spark/data",
)

def layer_path(layer: str, table: str) -> str:
  return f"{LAKEHOUSE_ROOT}/{layer}/{table}"
