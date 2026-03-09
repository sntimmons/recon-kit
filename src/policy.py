from dataclasses import dataclass, asdict
import json

@dataclass(frozen=True)
class MatchPolicy:
    # Matching controls
    auto_match_threshold: float = 92.0
    top_k: int = 2

    # Collision handling
    disambiguate_dupes_with_address: bool = True
    address_disambiguation_threshold: float = 95.0

    # Dominance-gap auto-approval (Block 2A)
    enable_dominance_auto_approve: bool = True
    dominance_min_c1: float = 88.0
    dominance_gap: float = 40.0
    dominance_max_c2: float = 60.0
    dominance_allowed_risks: tuple = ("low", "medium")  # never auto-approve "high"

    # Candidate blocking
    enable_blocking: bool = True
    block_lastname_prefix_len: int = 3

    # Weights
    w_name: float = 0.52
    w_dob: float = 0.25
    w_ssn4: float = 0.07
    w_hire: float = 0.05
    w_addr: float = 0.05
    w_state: float = 0.04
    w_city: float = 0.01
    w_worker_type: float = 0.01
    w_status: float = 0.00

    def to_json(self) -> str:
        return json.dumps(asdict(self), sort_keys=True)

