# Backtest: 2022 sanctions on Russia

Data: CEPII BACI bilateral trade in the graph (years available: 2018, 2019, 2020, 2022, 2023, 2024); World Bank GDP. Coalition = EU27 + US, UK, Japan, Canada, Australia, South Korea, Switzerland, Norway, New Zealand.

## Model prediction (from pre-shock data, 2020)

- Russia–coalition trade at risk: **$154.8B**
- Russia GDP 2020: $1,493B → exposure **10.4% of GDP** → severity **critical**
- Projected score changes at magnitude 1.0: trade vulnerability +0.41, economic influence −0.31, global risk +0.26
- Predicted direction: coalition trade falls sharply; part re-routes to non-sanctioning partners.

## What happened (BACI)

| Partner group | 2020 | 2023 | 2024 | change 2020→2024 |
|---|---|---|---|---|
| Sanctioning coalition | $154.8B | $31.0B | $15.7B | -90% |
| China | $73.9B | $143.5B | $147.5B | +100% |
| India | $7.1B | $11.1B | $11.3B | +58% |
| Türkiye | $13.6B | $25.3B | $19.5B | +43% |
| Kazakhstan | $16.9B | $23.1B | $24.7B | +46% |

## Verdict

- **Direction: correct.** Coalition trade changed -90%, the model predicted a collapse (exposure 10.4% of GDP, severity critical).
- **Re-routing: observed.** Trade grew with: China (+100%), India (+58%), Türkiye (+43%), Kazakhstan (+46%) — the model's cascade logic assumes partial re-routing (CASCADE_DAMPING = 0.5) and does not yet model trade *gains* for third countries.
- **Magnitude:** the model treats sanctioned trade as fully at risk at magnitude 1.0; the observed fall of 90% corresponds to magnitude ≈ 0.90 (sanctions exempted energy and some goods at first).
- **Limits:** coefficients are assumptions, not estimated; prices (2022 energy spike) inflate USD values; BACI lags 1–2 years. This is a direction and order-of-magnitude check, not a forecast accuracy score.
