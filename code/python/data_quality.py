"""
Data Quality — Contrôles Great Expectations pour le pipeline SportDataSolution
===============================================================================
Ce module expose deux fonctions appelées depuis les scripts de pipeline :

  validate_bronze_batch(df_spark)
      Valide un micro-batch Bronze (données brutes décodées depuis Redpanda).
      Appelé dans redpanda_to_s3_parquet.py via foreachBatch.

  validate_silver(df_spark)
      Valide le DataFrame Silver enrichi avant écriture sur S3.
      Appelé dans streaming_silver_mobilite_sport.py.

Les deux fonctions utilisent un contexte GX éphémère (aucun fichier écrit sur
disque) avec un datasource pandas (conversion toPandas() du batch Spark).
Les résultats sont loggués ; une anomalie n'interrompt pas le pipeline.
"""

import great_expectations as gx


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _log_results(layer: str, results) -> None:
    """Affiche les résultats GX de façon lisible dans les logs Spark."""
    success = results.success
    print(f"\n[QC {layer}] {'✓ Tous les contrôles sont OK' if success else '✗ Des anomalies ont été détectées'}")
    for vr in results.run_results.values():
        for res in vr.results:
            status = "OK" if res.success else "KO"
            col    = res.expectation_config.kwargs.get("column", "—")
            etype  = res.expectation_config.type
            print(f"  [{status}] {col} — {etype}")
            if not res.success and res.result:
                unexpected = res.result.get("unexpected_count", "?")
                total      = res.result.get("element_count", "?")
                pct        = res.result.get("unexpected_percent")
                pct_str    = f" ({pct:.1f}%)" if pct is not None else ""
                print(f"         → {unexpected}/{total} valeurs non conformes{pct_str}")
    print()


def _build_checkpoint(context, df_pandas, suite_name: str, expectations: list):
    """Construit et exécute un checkpoint GX éphémère sur un DataFrame pandas."""
    ds         = context.data_sources.add_pandas(name=suite_name)
    asset      = ds.add_dataframe_asset(name="batch")
    batch_def  = asset.add_batch_definition_whole_dataframe(name="batch_def")

    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
    for exp in expectations:
        suite.add_expectation(exp)

    vd = context.validation_definitions.add(
        gx.ValidationDefinition(name=suite_name, data=batch_def, suite=suite)
    )
    checkpoint = context.checkpoints.add(
        gx.Checkpoint(name=suite_name, validation_definitions=[vd])
    )
    return checkpoint.run(batch_parameters={"dataframe": df_pandas})


# ---------------------------------------------------------------------------
# Bronze — données brutes décodées (redpanda_to_s3_parquet.py)
# ---------------------------------------------------------------------------

def validate_bronze_batch(df_spark) -> bool:
    """
    Valide un micro-batch Bronze après décodage Debezium.

    Seuls les enregistrements actifs (is_deleted=False) sont validés :
    les événements de suppression CDC ont des champs métier nuls par nature.

    Contrôles (enregistrements actifs uniquement) :
    - employe_id             : non null
    - kafka_ts               : non null
    - distance_km            : non null, >= 0.01 km
    - type_pratique_sportive : dans l'ensemble des valeurs autorisées

    Retourne True si tous les contrôles passent.
    """
    if df_spark.isEmpty():
        return True

    # Filtrer les suppressions CDC : leurs champs métier sont nuls par définition
    df_active = df_spark.filter(df_spark["is_deleted"] == False)
    if df_active.isEmpty():
        print("[QC Bronze] Uniquement des suppressions CDC — validation ignorée.")
        return True

    df_pandas = df_active.toPandas()
    context   = gx.get_context(mode="ephemeral")

    expectations = [
        gx.expectations.ExpectColumnValuesToNotBeNull(column="employe_id"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="kafka_ts"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="distance_km"),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="distance_km",
            min_value=0.01,
            max_value=None,
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="type_pratique_sportive",
            value_set=["Course à pied", "Vélo", "Natation", "Marche", "Trotinette", "Voile"],
        ),
    ]

    results = _build_checkpoint(context, df_pandas, "bronze_suite", expectations)
    _log_results("Bronze", results)
    return results.success


# ---------------------------------------------------------------------------
# Silver — données enrichies (streaming_silver_mobilite_sport.py)
# ---------------------------------------------------------------------------

def validate_silver(df_spark) -> bool:
    """
    Valide le DataFrame Silver avant écriture sur S3.

    Contrôles :
    - employe_id                : non null
    - pratique_sportive_annuelle: >= 0
    - coherence_distance        : dans ['OK', 'ERREUR'] ou null
                                  (null = employé non mobilité douce)
    - distance_km               : >= 0 quand renseignée
                                  (null autorisé pour non mobilité douce)

    Retourne True si tous les contrôles passent.
    """
    if df_spark.isEmpty():
        return True

    df_pandas = df_spark.toPandas()
    context   = gx.get_context(mode="ephemeral")

    expectations = [
        gx.expectations.ExpectColumnValuesToNotBeNull(column="employe_id"),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="pratique_sportive_annuelle",
            min_value=0,
            max_value=None,
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="coherence_distance",
            value_set=["OK", "ERREUR", None],
            mostly=1.0,
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="distance_km",
            min_value=0,
            max_value=None,
            mostly=1.0,          # null autorisé (non mobilité douce)
        ),
    ]

    results = _build_checkpoint(context, df_pandas, "silver_suite", expectations)
    _log_results("Silver", results)
    return results.success
