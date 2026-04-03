import pandas as pd
import networkx as nx
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

    # Export CSV
    edges_df.to_csv("variant_drug_edges.csv", index=False)
    print("Saved to variant_drug_edges.csv")

    # Create graph
    G = nx.Graph()

    for _, row in edges_df.iterrows():
        G.add_node(row["source"], node_type="variant", gene_symbol=row["gene_symbol"])
        G.add_node(row["target"], node_type="drug")

        G.add_edge(
            row["source"],
            row["target"],
            pharmgkb_annotation_id=row["pharmgkb_annotation_id"],
            level_of_evidence=row["level_of_evidence"],
            evidence_score=row["evidence_score"],
            phenotype_category=row["phenotype_category"],
            phenotypes=row["phenotypes"],
            pmid_count=row["pmid_count"],
            evidence_count=row["evidence_count"],
            specialty_population=row["specialty_population"],
            last_updated=row["last_updated"],
        )

    print("Number of nodes:", G.number_of_nodes())
    print("Number of edges in graph:", G.number_of_edges())

    # Top variants have the most drug 
    variant_degrees = []
    for node, degree in G.degree():
        if G.nodes[node].get("node_type") == "variant":
            variant_degrees.append((node, degree, G.nodes[node].get("gene_symbol")))

    variant_degrees = sorted(variant_degrees, key=lambda x: x[1], reverse=True)

    print("\nTop 10 most connected variants:")
    for variant, deg, gene in variant_degrees[:10]:
        print(f"{variant} ({gene}) -> {deg}")

if __name__ == "__main__":
    main()