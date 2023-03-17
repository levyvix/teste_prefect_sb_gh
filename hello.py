from prefect import flow, task
import polars as pl


@task
def read_data(path) -> pl.DataFrame:
    return pl.scan_csv(path)


@task
def preprocess(data) -> pl.DataFrame:
    # add 10 years to every age
    return data.with_columns([
        pl.col("age") + 15,
        pl.col('name').str.to_lowercase().str.contains('a').alias('has_a'),
    ]).with_columns([
        (pl.col('age') % 4 == 0).alias('age_div_4'),
    ])


@task(log_prints=True)
def write_data(data: pl.DataFrame) -> None:
    result = data
    result.write_parquet("data.parquet")

@flow(name="Polars Data Processing")
def main() -> None:
    data = read_data("data.csv")

    preprocesed_data = preprocess(data)

    write_data(preprocesed_data)


if __name__ == "__main__":
    main()
