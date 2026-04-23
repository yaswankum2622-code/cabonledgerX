"""Synthetic company-panel generator for downstream modeling scaffolds."""

from __future__ import annotations

from random import Random

import pandas as pd

from carbonledgerx.models.canonical_tables import ProcessedTableArtifact


SECTOR_CONFIG = {
    "Utilities": {
        "revenue_range": (900, 18000),
        "scope1_factor": (140, 320),
        "scope2_factor": (35, 95),
        "renewable_share": (15, 75),
        "fleet_electrification": (5, 35),
        "growth_pct": (-1.5, 4.5),
        "commitment_probability": 0.78,
    },
    "Materials": {
        "revenue_range": (250, 9000),
        "scope1_factor": (120, 260),
        "scope2_factor": (28, 80),
        "renewable_share": (8, 45),
        "fleet_electrification": (3, 25),
        "growth_pct": (-2.0, 5.5),
        "commitment_probability": 0.52,
    },
    "Manufacturing": {
        "revenue_range": (180, 12000),
        "scope1_factor": (55, 165),
        "scope2_factor": (18, 65),
        "renewable_share": (10, 55),
        "fleet_electrification": (5, 30),
        "growth_pct": (-1.0, 7.0),
        "commitment_probability": 0.63,
    },
    "Logistics": {
        "revenue_range": (120, 8500),
        "scope1_factor": (70, 180),
        "scope2_factor": (10, 38),
        "renewable_share": (6, 35),
        "fleet_electrification": (8, 55),
        "growth_pct": (-0.5, 8.0),
        "commitment_probability": 0.58,
    },
    "Technology": {
        "revenue_range": (150, 25000),
        "scope1_factor": (3, 18),
        "scope2_factor": (4, 22),
        "renewable_share": (35, 95),
        "fleet_electrification": (20, 75),
        "growth_pct": (1.0, 12.0),
        "commitment_probability": 0.86,
    },
    "Retail": {
        "revenue_range": (90, 16000),
        "scope1_factor": (6, 35),
        "scope2_factor": (8, 30),
        "renewable_share": (18, 70),
        "fleet_electrification": (10, 50),
        "growth_pct": (0.0, 8.5),
        "commitment_probability": 0.72,
    },
    "Healthcare": {
        "revenue_range": (120, 14000),
        "scope1_factor": (5, 24),
        "scope2_factor": (7, 26),
        "renewable_share": (22, 75),
        "fleet_electrification": (8, 40),
        "growth_pct": (0.5, 8.0),
        "commitment_probability": 0.74,
    },
    "Consumer Goods": {
        "revenue_range": (100, 11000),
        "scope1_factor": (10, 48),
        "scope2_factor": (9, 28),
        "renewable_share": (18, 68),
        "fleet_electrification": (12, 45),
        "growth_pct": (0.0, 7.5),
        "commitment_probability": 0.69,
    },
}

COUNTRIES = [
    "United States",
    "United Kingdom",
    "Germany",
    "France",
    "Canada",
    "India",
    "Japan",
    "Australia",
    "Brazil",
    "Singapore",
]

NAME_PREFIXES = [
    "Northstar",
    "Summit",
    "BlueRiver",
    "Apex",
    "Harbor",
    "Lattice",
    "Evergreen",
    "Ironwood",
    "Aurora",
    "Cobalt",
    "Meridian",
    "Atlas",
    "Vertex",
    "Granite",
    "Pioneer",
]

NAME_SUFFIXES = {
    "Utilities": ["Grid", "Power", "Energy"],
    "Materials": ["Minerals", "Materials", "Industrial"],
    "Manufacturing": ["Works", "Manufacturing", "Fabrication"],
    "Logistics": ["Logistics", "Transit", "Freight"],
    "Technology": ["Systems", "Cloud", "Digital"],
    "Retail": ["Retail", "Commerce", "Markets"],
    "Healthcare": ["Health", "Life Sciences", "Care"],
    "Consumer Goods": ["Brands", "Foods", "Products"],
}


def build_synthetic_company_panel(
    *,
    n_companies: int = 500,
    seed: int = 20250423,
) -> ProcessedTableArtifact:
    """Build a reproducible synthetic annual company panel."""

    rng = Random(seed)
    sectors = list(SECTOR_CONFIG.keys())
    rows: list[dict[str, object]] = []

    for index in range(1, n_companies + 1):
        sector = rng.choice(sectors)
        config = SECTOR_CONFIG[sector]
        country = rng.choice(COUNTRIES)
        revenue_usd_m = round(rng.uniform(*config["revenue_range"]), 2)
        base_year = rng.randint(2019, 2023)
        climate_commitment_flag = rng.random() < config["commitment_probability"]
        target_year = base_year + rng.randint(5, 12 if climate_commitment_flag else 8)
        target_reduction_pct = round(
            rng.uniform(22, 68) if climate_commitment_flag else rng.uniform(5, 24),
            1,
        )
        renewable_share_pct = round(rng.uniform(*config["renewable_share"]), 1)
        fleet_electrification_pct = round(rng.uniform(*config["fleet_electrification"]), 1)
        annual_activity_growth_pct = round(rng.uniform(*config["growth_pct"]), 1)

        scope1_base = revenue_usd_m * rng.uniform(*config["scope1_factor"])
        scope2_lb_base = revenue_usd_m * rng.uniform(*config["scope2_factor"])
        current_scope1_tco2e = round(scope1_base * rng.uniform(0.75, 1.25), 0)
        current_scope2_lb_tco2e = round(scope2_lb_base * rng.uniform(0.8, 1.2), 0)

        market_based_multiplier = max(
            0.08,
            1.0 - (renewable_share_pct / 100.0) * rng.uniform(0.55, 0.92),
        )
        current_scope2_mb_tco2e = round(current_scope2_lb_tco2e * market_based_multiplier, 0)

        rows.append(
            {
                "company_id": f"SYN{index:04d}",
                "company_name": _build_company_name(rng, sector, index),
                "sector": sector,
                "country": country,
                "revenue_usd_m": revenue_usd_m,
                "base_year": base_year,
                "target_year": target_year,
                "target_reduction_pct": target_reduction_pct,
                "current_scope1_tco2e": current_scope1_tco2e,
                "current_scope2_lb_tco2e": current_scope2_lb_tco2e,
                "current_scope2_mb_tco2e": current_scope2_mb_tco2e,
                "annual_activity_growth_pct": annual_activity_growth_pct,
                "renewable_share_pct": renewable_share_pct,
                "fleet_electrification_pct": fleet_electrification_pct,
                "climate_commitment_flag": climate_commitment_flag,
                "modeled_disclosure_claim": _build_disclosure_claim(
                    rng,
                    climate_commitment_flag=climate_commitment_flag,
                    renewable_share_pct=renewable_share_pct,
                    fleet_electrification_pct=fleet_electrification_pct,
                ),
            }
        )

    dataframe = pd.DataFrame(rows).convert_dtypes()
    selected_key_fields = [
        "company_id",
        "company_name",
        "sector",
        "country",
        "base_year",
        "target_year",
        "target_reduction_pct",
        "climate_commitment_flag",
    ]
    assumptions = [
        f"Generated {n_companies} synthetic annual company records with fixed random seed {seed}.",
        "Applied sector-conditioned revenue, emissions, renewable share, fleet electrification, and growth ranges.",
        "Modeled current_scope2_mb_tco2e as less than or equal to current_scope2_lb_tco2e based on renewable share assumptions.",
    ]
    return ProcessedTableArtifact(
        output_name="company_synthetic_panel.parquet",
        dataframe=dataframe,
        selected_key_fields=selected_key_fields,
        assumptions=assumptions,
        source_inputs=[],
    )


def _build_company_name(rng: Random, sector: str, index: int) -> str:
    """Build a deterministic-looking synthetic company name."""

    prefix = rng.choice(NAME_PREFIXES)
    suffix = rng.choice(NAME_SUFFIXES[sector])
    legal_suffix = rng.choice(["Holdings", "Group", "Ltd", "Inc", "plc"])
    return f"{prefix} {suffix} {legal_suffix} {index:03d}"


def _build_disclosure_claim(
    rng: Random,
    *,
    climate_commitment_flag: bool,
    renewable_share_pct: float,
    fleet_electrification_pct: float,
) -> str:
    """Generate a simple modeled disclosure claim."""

    if climate_commitment_flag:
        options = [
            "Public science-based target",
            "Net-zero transition plan",
            "Operational decarbonization program",
        ]
        if renewable_share_pct >= 60:
            options.append("High renewable electricity sourcing claim")
        if fleet_electrification_pct >= 40:
            options.append("Fleet electrification roadmap")
    else:
        options = [
            "General sustainability statement",
            "Energy efficiency narrative",
            "No formal climate target disclosed",
        ]

    return rng.choice(options)
