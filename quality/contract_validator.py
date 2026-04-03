import yaml
from pathlib import Path


def load_contract(name: str) -> dict:
    path = Path(f"contracts/{name}.yml")
    with open(path) as f:
        return  yaml.safe_load(f)


def validate(data: dict, contract_name: str) -> list[str]:
    """
    Возвращает список нарушений. Пустой список = всё ок.
    """
    contract = load_contract(contract_name)
    violations = []

    for field in contract["fields"]:
        name = field["name"]
        alias = field["alias"]
        value = data.get(name)

        for constraint in field.get("constraints", []):
            if constraint == "not_null" and value is None:
                violations.append(f"{alias}: not_null violation")

            elif constraint == "numeric_string" and value is not None:
                try:
                    float(value)
                except ValueError:
                    violations.append(f"{alias}: must by numeric, got {value}")

            elif constraint == "positive" and value is not None:
                if float(value) <= 0:
                    violations.append(f"{alias}: must be positive, got {value}")

            elif isinstance(constraint, dict) and "accepted_values" in constraint:
                if value not in constraint["accepted_values"]:
                    violations.append(
                        f"{alias}: invalid value '{value}', "
                        f"expected one of {constraint['accepted_values']}"
                    )

            elif isinstance(constraint, dict) and "range" in constraint:
                low, high = constraint["range"]
                if not (low <= float(value) <= high):
                    violations.append(
                        f"{alias}: out of range [{low}, {high}], got {value}"
                    )

    return violations