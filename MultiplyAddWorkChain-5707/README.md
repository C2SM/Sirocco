# AiiDA Process Dump: MultiplyAddWorkChain <5707>

This directory contains files related to the AiiDA process node 5707.
- **UUID:** 15f70e5c-b8b1-4bdc-aea4-9f1847874d41
- **Type:** process.workflow.workchain.WorkChainNode.

Sub-directories (if present) represent called calculations or workflows, ordered by creation time.
File/directory structure within a calculation node:
- `inputs/`: Contains scheduler submission script (`_aiidasubmit.sh`), stdin file (`aiida.in`), and internal
AiiDA info (`.aiida/`).
- `outputs/`: Contains files retrieved by the parser (e.g., `aiida.out`, `_scheduler-stdout.txt`,
`_scheduler-stderr.txt`).
- `node_inputs/`: Contains repositories of input data nodes linked via `INPUT_CALC`.
- `node_outputs/`: Contains repositories of output data nodes linked via `CREATE` (excluding `retrieved`).
- `aiida_node_metadata.yaml`: Human-readable metadata, attributes, and extras of this node.

## Process Status (`verdi process status 5707`)

```
MultiplyAddWorkChain<5707> Finished [0] [3:result]
    ├── multiply<5708> Finished [0]
    └── ArithmeticAddCalculation<5710> Finished [0]
```

## Process Report (`verdi process report 5707`)

```
2025-11-04 13:30:09 [280820 | REPORT]: [5707|MultiplyAddWorkChain|add]: Submitted the `ArithmeticAddCalculation`: uuid: 581082fa-b651-4368-98e4-706e4c02edd3 (pk: 5710) (aiida.calculations:core.arithmetic.add)
```

## Node Info (`verdi node show 15f70e5c-b8b1-4bdc-aea4-9f1847874d41`)

```
Property     Value
-----------  ------------------------------------
type         MultiplyAddWorkChain
state        Finished [0]
pk           5707
uuid         15f70e5c-b8b1-4bdc-aea4-9f1847874d41
label
description
ctime        2025-11-04 13:30:08.690580+00:00
mtime        2025-11-04 13:30:16.193564+00:00

Inputs      PK  Type
--------  ----  -------------
code      5697  InstalledCode
x         5704  Int
y         5705  Int
z         5706  Int

Outputs      PK  Type
---------  ----  ------
result     5713  Int

Called      PK  Type
--------  ----  ------------------------
CALL      5708  multiply
CALL      5710  ArithmeticAddCalculation

Log messages
---------------------------------------------
There are 1 log messages for this calculation
Run 'verdi process report 5707' to see them
```
