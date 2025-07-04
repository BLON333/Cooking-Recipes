from enum import Enum


class SkipReason(Enum):
    """Standard reasons for skipping a bet across the pipeline."""

    LOW_INITIAL = "low_initial"
    LOW_TOPUP = "low_topup"
    ALREADY_LOGGED = "already_logged"
    MARKET_NOT_MOVED = "market_not_moved"
    NO_CONSENSUS = "no_consensus"
    NO_WEBHOOK = "no_webhook"
    ODDS_WORSENED = "odds_worsened"
    QUIET_HOURS = "quiet_hours"
    UNKNOWN = "skipped"
