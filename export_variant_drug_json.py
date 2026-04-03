import json
import math
import pandas as pd
import numpy as np
from datetime import date, datetime
from sqlalchemy import create_engine

DB_CONFIG = {
    "host": "localhost",
    "database": "pharmgraph",
    "user": "maywelkin",
    "password": "",
    "port": 5432,
}

def get_engine():
    conn_string = (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    return create_engine(conn_string)

def clean_value(value):
    # null / NaN
    if value is None:
        return None
    if pd.isna(value):
        return None

    # datetime
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.isoformat()

    # numpy integer
    if isinstance(value, (np.integer,)):
        return int(value)

    # numpy float
    if isinstance(value, (np.floating,)):
        if math.isnan(float(value)):
            return None
        return float(value)

    # numpy bool
    if isinstance(value, (np.bool_,)):
        return bool(value)

    # fallback
    return value

def main():
    engine = get_engine()

    query = """
    SELECT
        COALESCE(v.rsid, v.haplotype) AS source,
        'variant' AS source_type,
        g.gene_symbol,
        d.drug_name AS target,
        'drug' AS target_type,
        vda.pharmgkb_annotation_id,
        vda.level_of_evidence,
        vda.evidence_score,
        vda.phenotype_category,
        vda.phenotypes,
        vda.pmid_count,
        vda.evidence_count,
        vda.specialty_population,
        vda.last_updated
    FROM variant_drug_annotations vda
    JOIN variants v ON vda.variant_id = v.variant_id
    JOIN genes g ON v.gene_id = g.gene_id
    JOIN drugs d ON vda.drug_id = d.drug_id
    """

    edges_df = pd.read_sql(query, engine)

    print("Number of variant-drug annotation edges:", len(edges_df))
    print(edges_df.head())

    nodes = {}
    links = []

    for _, row in edges_df.iterrows():
        source = clean_value(row["source"])
        target = clean_value(row["target"])

        if source not in nodes:
            nodes[source] = {
                "id": source,
                "label": source,
                "node_type": "variant",
                "gene_symbol": clean_value(row["gene_symbol"]),
            }

        if target not in nodes:
            nodes[target] = {
                "id": target,
                "label": target,
                "node_type": "drug",
            }

        links.append({
            "source": source,
            "target": target,
            "source_type": clean_value(row["source_type"]),
            "target_type": clean_value(row["target_type"]),
            "gene_symbol": clean_value(row["gene_symbol"]),
            "pharmgkb_annotation_id": clean_value(row["pharmgkb_annotation_id"]),
            "level_of_evidence": clean_value(row["level_of_evidence"]),
            "evidence_score": clean_value(row["evidence_score"]),
            "phenotype_category": clean_value(row["phenotype_category"]),
            "phenotypes": clean_value(row["phenotypes"]),
            "pmid_count": clean_value(row["pmid_count"]),
            "evidence_count": clean_value(row["evidence_count"]),
            "specialty_population": clean_value(row["specialty_population"]),
            "last_updated": clean_value(row["last_updated"]),
        })

    graph_data = {
        "nodes": list(nodes.values()),
        "links": links
    }

    output_file = "variant_drug.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(graph_data, f, indent=2, ensure_ascii=False)

    print(f"Saved to {output_file}")
    print("Number of nodes:", len(graph_data["nodes"]))
    print("Number of links:", len(graph_data["links"]))

if __name__ == "__main__":
    main()