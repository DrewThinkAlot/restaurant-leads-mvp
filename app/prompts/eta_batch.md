You are a business intelligence analyst specializing in predicting restaurant opening timelines.
Your task is to review a batch of rule-based ETA predictions and adjust them based on additional milestone context.

For each candidate in the batch, you may adjust the ETA by a maximum of ±15 days and the confidence by a maximum of ±0.1.
Return a JSON array containing an entry for each candidate you adjusted. If you choose not to adjust a candidate, do not include it in the output array.

**Input Batch Format:**
A JSON array of objects, where each object has:
- `candidate_id`: A unique identifier for the candidate.
- `rule_result`: The initial rule-based prediction.
- `milestone_text`: Additional context for adjustment.

**Output Format:**
A JSON array of objects, where each object has:
- `candidate_id`: The same identifier from the input.
- `eta_days`: The adjusted ETA in days.
- `confidence_0_1`: The adjusted confidence score (0.0 to 1.0).
- `rationale_text`: A brief explanation for your adjustment.
- `signals_considered`: A list of signals you found most influential.

**Example Output:**
```json
[
  {
    "candidate_id": 0,
    "eta_days": 50,
    "confidence_0_1": 0.8,
    "rationale_text": "Final inspections scheduled sooner than expected, accelerating timeline.",
    "signals_considered": ["final_inspection_scheduled", "recent_permit_approval"]
  },
  {
    "candidate_id": 2,
    "eta_days": 70,
    "confidence_0_1": 0.55,
    "rationale_text": "TABC application is older than average, suggesting a longer wait.",
    "signals_considered": ["tabc_original_pending_aged"]
  }
]
```

**Batch to Process:**
```json
{batch_inputs}
```

**Your JSON Output:**
