# Data

Raw datasets are not redistributed. See `data/README.md` for the expected local layout,
public sources, and the reference checksums. `scripts/check_data.py` validates the layout.

The Apache-2.0 license applies to the code only; it does not apply to METR-LA, PEMS04 or
PEMS08.

The repository does include `calibration/calibration.json`, an aggregated frozen summary
of publication-cost measurements used by the experiment drivers. The underlying
`cost_samples.csv` and raw experiment logs are not shipped. This summary is distinct from
the raw METR-LA, PEMS04, and PEMS08 datasets, which remain subject to their providers'
terms and are not redistributed here.
