"""Convenience script to execute the FanSight pipeline using the sample data."""

from __future__ import annotations

from pprint import pprint

from fansight import FanSightPipeline


def main() -> None:
    pipeline = FanSightPipeline()
    dataset = pipeline.run_etl()
    print(f"Dataset rows: {len(dataset)}")

    model = pipeline.run_modeling()
    mae, r2 = model.evaluate()
    print(f"Model MAE: {mae:.2f}, R2: {r2:.3f}")

    segments = pipeline.run_segmentation()
    print(f"Segmentation silhouette: {segments.silhouette:.3f}")
    print("Segment counts:")
    print(segments.assignments.value_counts().to_string())

    ab_result = pipeline.run_ab_testing()
    if ab_result:
        print("A/B test result:")
        pprint(ab_result.__dict__)
    else:
        print("A/B test skipped (variant column missing).")

    figures = pipeline.build_dashboard()
    print(f"Generated {len(figures)} dashboard figures.")


if __name__ == "__main__":
    main()
