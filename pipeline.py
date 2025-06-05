import pandas as pd
from pandas import DataFrame
from pandas.tseries.offsets import MonthEnd


def create_patient_enrollment_span(
        patient_month_df: DataFrame
) -> DataFrame:
    df = patient_month_df.copy()

    # Convert month_year column into proper datetime
    df['month'] = pd.to_datetime(df['month_year'], format="%m/%d/%Y")

    # ORDER BY patient_id and month
    df = df.sort_values(['patient_id', 'month']).reset_index(drop=True)

    # Convert month datetime to monthly Period
    df['period'] = df['month'].dt.to_period('M')

    # LAG period for a given patient to be able to determine connected coverage months
    df['prev_period'] = df.groupby('patient_id')['period'].shift(1)

    # Mark if there is a gap in coverage or not
    df['gap'] = (df['period'] != df['prev_period'] + 1)

    # Partition into enrollment groups. If there is a gap in coverage, increment into a new enrollment_group.
    df['enrollment_group'] = df.groupby('patient_id')['gap'].cumsum()

    # GROUP BY patient_id & enrollment_group to get MIN/MAX month per enrollment_group
    enrollment_ranges = (
        df
        .groupby(['patient_id', 'enrollment_group'])
        .agg(
            enrollment_start_date=('month', 'min'),
            enrollment_end_date=('month', 'max')
        )
        .reset_index()
    )

    # Get last day of month for enrollment_end_date
    enrollment_ranges['enrollment_end_date'] = (enrollment_ranges['enrollment_end_date'] + MonthEnd(1))

    patient_enrollment_span_df = enrollment_ranges[[
        'patient_id',
        'enrollment_start_date',
        'enrollment_end_date'
    ]]
    return patient_enrollment_span_df


def create_result(
        patient_enrollment_span_df: DataFrame,
        outpatient_visits_df: DataFrame
) -> DataFrame:
    op_df = outpatient_visits_df.copy()

    # Convert date column into proper datetime
    op_df["date"] = pd.to_datetime(op_df["date"], format="%m/%d/%Y")

    # JOIN patient_enrollment_span to outpatient_visits_file
    merged = pd.merge(
        op_df,
        patient_enrollment_span_df,
        on="patient_id",
        how="inner"
    )

    # Keep only rows where date falls in between enrollment period
    mask = (
        (merged["date"] >= merged["enrollment_start_date"]) &
        (merged["date"] <= merged["enrollment_end_date"])
    )
    filtered = merged.loc[mask]

    # GROUP BY patient_id, enrollment_start_date, enrollment_end_date
    # ct_outpatient_visits -> SUM of outpatient_visit_count
    # ct_days_with_outpatient_visit -> COUNT DISTINCT days within enrollment period with at least 1 OP visit
    result_df = (
        filtered
        .groupby(
            ["patient_id", "enrollment_start_date", "enrollment_end_date"],
            as_index=False
        )
        .agg(
            ct_outpatient_visits=("outpatient_visit_count", "sum"),
            ct_days_with_outpatient_visit=("date", lambda x: x.dt.normalize().nunique())
        )
    )
    return result_df


if __name__ == "__main__":
    # Load patient_id_month_year.csv into DataFrame
    patient_id_month_year_df = pd.read_csv("patient_id_month_year.csv")

    # Run transform logic to generate patient_enrollment_span DataFrame
    patient_enrollment_span = create_patient_enrollment_span(patient_month_df=patient_id_month_year_df)

    # Write patient_enrollment_span DataFrame to patient_enrollment_span.csv file
    patient_enrollment_span.to_csv("patient_enrollment_span.csv", index=False)

    # Load outpatient_visits_file.csv to op_visits
    op_visits = pd.read_csv("outpatient_visits_file.csv")

    # Run transform logic to generate result DataFrame
    result = create_result(
        patient_enrollment_span_df=patient_enrollment_span,
        outpatient_visits_df=op_visits
    )

    # Write result DataFrame to result.csv file
    result.to_csv("result.csv", index=False)

    # Print Answer 1: Row Count for patient_enrollment_span.csv
    row_count = len(patient_enrollment_span)
    print(f"Answer 1 | Total rows in patient_enrollment_span: {row_count}")

    # Print Answer 2: Distinct Count for ct_days_with_outpatient_visit in result.csv
    distinct_count = result['ct_days_with_outpatient_visit'].nunique()
    print(f"Answer 2 | Number of distinct ct_days_with_outpatient_visit: {distinct_count}")
