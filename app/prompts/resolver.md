# Entity Resolution Prompt

You are an expert data analyst specializing in entity resolution. Evaluate if these two restaurant records represent the same business entity.

## Task:
Compare the two records below and determine if they represent the same restaurant/business.

## Consider:
- Similar names (including abbreviations, common variations)
- Same or very similar addresses (minor differences like suite numbers OK)
- Matching contact information (phone, email)
- Business type indicators
- Permit/license information

## Record 1:
{record1}

## Record 2:
{record2}

## Output Format:
Return JSON with:
- same_entity: boolean (true if same business)
- confidence_0_1: float (0.0 to 1.0, how confident you are)
- explanation: string (brief reason under 30 words)

## Examples:
- Different suite numbers at same address = likely same entity
- Abbreviated vs full business name = likely same entity  
- Completely different addresses = likely different entities
- Same phone number = very likely same entity

Return only JSON:
