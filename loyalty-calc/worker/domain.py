from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN


@dataclass(frozen=True)
class Decision:
    status: str            
    points_delta: int
    reject_reason: str | None


def purchase_points(amount: Decimal) -> int:
    """5 % от суммы чека, дробная часть отбрасывается."""
    return int((amount * Decimal("0.05")).to_integral_value(rounding=ROUND_DOWN))


def decide_purchase(amount: Decimal, duplicate: bool) -> Decision:
    if duplicate:
        return Decision("rejected", 0, "duplicate_purchase")
    return Decision("applied", purchase_points(amount), None)


def decide_return(
    purchase_amount: Decimal | None,
    purchase_customer: str | None,
    customer_id: str,
    already_returned: bool,
) -> Decision:
    if purchase_amount is None:
        return Decision("rejected", 0, "purchase_not_found")
    if purchase_customer != customer_id:
        return Decision("rejected", 0, "customer_mismatch")
    if already_returned:
        return Decision("rejected", 0, "already_returned")
    return Decision("applied", -purchase_points(purchase_amount), None)


def decide_redeem(amount: Decimal, points: int, current_balance: Decimal) -> Decision:
    limit = (amount * Decimal("0.30"))
    if Decimal(points) > limit:
        return Decision("rejected", 0, "limit_exceeded")
    if Decimal(points) > current_balance:
        return Decision("rejected", 0, "insufficient_balance")
    return Decision("applied", -points, None)