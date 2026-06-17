# Commands

## Create local image
```
docker image build --no-cache -t rm-allocation-process-inputs-image .
```
## Tag docker local image
```
docker tag rm-allocation-process-inputs-image 471112573018.dkr.ecr.ap-south-1.amazonaws.com/rm-allocation-serverless-funcs_rm-allocation-process-inputs:latest
```

## Push docker image
```
docker push 471112573018.dkr.ecr.ap-south-1.amazonaws.com/rm-allocation-serverless-funcs_rm-allocation-process-inputs:latest
```

# Details
## Lambda function name
```
rm-allocation-serverless-funcs_rm-allocation-process-inputs
```

## Two-pass portfolio coordination (direct routes)

Direct-route flights are processed in two SQS-driven passes:

1. **Fare-anchor pass** (`pass = "anchor"`): Runs Phases A–J and the candidate-selection portion of Phase K. Stops once `strategyFare` is known. The result is stored in a Redis hash keyed by `(run_id, sector, departure_date)`.

2. **Allocation pass** (`pass = "allocation"`): Triggered automatically once all flights in a portfolio have completed their fare-anchor pass. Carries portfolio-level aggregates so each flight can record z-scores and a `Portfolio_Anchor` for analysis. Runs the ladder lookup (Phase K tail), AU grid build (Phase L), Navitaire push (Phase M), and summary write (Phase N).

**Single-flight short-circuit**: When `portfolio_count == 1`, both passes run in-process within the same Lambda invocation — no Redis or SQS coordination needed.

**Connections and sales-inventory routes** are completely unaffected by this change — they continue to use the existing single-message flow.

For full details, see `ARCHITECTURE.md` §12 in `.kiro/specs/portfolio-anchor-split/`.

