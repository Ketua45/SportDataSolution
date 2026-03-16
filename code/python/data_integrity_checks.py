import warnings
warnings.filterwarnings("ignore")

import great_expectations as gx
from dotenv import load_dotenv
import os

load_dotenv()
print("Great Expectations version :", gx.__version__, "\n\n")
print("Début des contrôles de cohérence sur la table pratique_sport :\n")
print("-------------------------------------------------------------\n")
context = gx.get_context()

connection_string = os.getenv("DATABASE_URL")

datasource = context.data_sources.add_sql(
    name="SportDataSolution",
    connection_string=connection_string,
)

data_asset = datasource.add_table_asset(
    name="pratique_sport",
    table_name="pratique_sport",
    schema_name="public",
)

batch_definition = data_asset.add_batch_definition_whole_table(
    name="batch_complet"
)

suite = context.suites.add(
    gx.ExpectationSuite(name="controles_coherence")
)

suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(
        column="distance",
        min_value=0.01,
        max_value=None,
    )
)

suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeInSet(
        column="type_pratique_sportive",
        value_set=["Course à pied", "Vélo", "Natation", "Marche", "Trotinette", "Voile"],
    )
)

vd = context.validation_definitions.add(
    gx.ValidationDefinition(
        name="validation_sport",
        data=batch_definition,
        suite=suite,
    )
)

checkpoint = context.checkpoints.add(
    gx.Checkpoint(
        name="checkpoint_sport",
        validation_definitions=[vd],
    )
)

results = checkpoint.run()
print("Succès :", results.success)
print()

for vr in results.run_results.values():
    for res in vr.results:
        status = "-- OK --" if res.success else "-- KO --"
        exp_type = res.expectation_config.type
        col = res.expectation_config.kwargs.get("column", "N/A")
        details = {k: v for k, v in res.expectation_config.kwargs.items() if k != "column"}
        print(f"{status} [{col}] {exp_type}")
        print(f"   Paramètres : {details}")
        if not res.success and res.result:
            unexpected = res.result.get("unexpected_count", "?")
            total = res.result.get("element_count", "?")
            pct = res.result.get("unexpected_percent", "?")
            print(f"   Valeurs non conformes : {unexpected}/{total} ({pct:.1f}%)")
        print()
