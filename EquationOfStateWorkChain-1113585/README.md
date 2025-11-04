# AiiDA Process Dump: EquationOfStateWorkChain <1113585>

This directory contains files related to the AiiDA process node 1113585.
- **UUID:** d4d3af69-e1d4-47ce-8f4e-b28cde53c0b3
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

## Process Status (`verdi process status 1113585`)

```
EquationOfStateWorkChain<1113585> Finished [0] [3:inspect_eos]
    ├── scale_structure<1113746> Finished [0]
    ├── QuantumEspressoCommonRelaxWorkChain<1114126> Finished [0] [2:convert_outputs]
    │   ├── PwRelaxWorkChain<1114384> Finished [0] [3:results]
    │   │   └── PwBaseWorkChain<1115624> Finished [0] [5:results]
    │   │       ├── create_kpoints_from_distance<1116946> Finished [0]
    │   │       └── PwCalculation<1117133> Finished [0]
    │   ├── extract_from_parameters<1124035> Finished [0]
    │   └── extract_from_trajectory<1124124> Finished [0]
    ├── scale_structure<1125111> Finished [0]
    ├── QuantumEspressoCommonRelaxWorkChain<1125160> Finished [0] [2:convert_outputs]
    │   ├── PwRelaxWorkChain<1125552> Finished [0] [3:results]
    │   │   └── PwBaseWorkChain<1132127> Finished [0] [5:results]
    │   │       └── PwCalculation<1132581> Finished [0]
    │   ├── extract_from_parameters<1140733> Finished [0]
    │   └── extract_from_trajectory<1140783> Finished [0]
    ├── scale_structure<1125162> Finished [0]
    ├── QuantumEspressoCommonRelaxWorkChain<1125214> Finished [0] [2:convert_outputs]
    │   ├── PwRelaxWorkChain<1125394> Finished [0] [3:results]
    │   │   └── PwBaseWorkChain<1125456> Finished [0] [5:results]
    │   │       └── PwCalculation<1126000> Finished [0]
    │   ├── extract_from_parameters<1133318> Finished [0]
    │   └── extract_from_trajectory<1133986> Finished [0]
    ├── scale_structure<1125219> Finished [0]
    ├── QuantumEspressoCommonRelaxWorkChain<1125318> Finished [0] [2:convert_outputs]
    │   ├── PwRelaxWorkChain<1125650> Finished [0] [3:results]
    │   │   └── PwBaseWorkChain<1132140> Finished [0] [5:results]
    │   │       └── PwCalculation<1132744> Finished [0]
    │   ├── extract_from_parameters<1140815> Finished [0]
    │   └── extract_from_trajectory<1140899> Finished [0]
    ├── scale_structure<1125322> Finished [0]
    ├── QuantumEspressoCommonRelaxWorkChain<1125353> Finished [0] [2:convert_outputs]
    │   ├── PwRelaxWorkChain<1126816> Finished [0] [3:results]
    │   │   └── PwBaseWorkChain<1126890> Finished [0] [5:results]
    │   │       └── PwCalculation<1127194> Finished [0]
    │   ├── extract_from_parameters<1135801> Finished [0]
    │   └── extract_from_trajectory<1135829> Finished [0]
    ├── scale_structure<1125357> Finished [0]
    ├── QuantumEspressoCommonRelaxWorkChain<1126742> Finished [0] [2:convert_outputs]
    │   ├── PwRelaxWorkChain<1126831> Finished [0] [3:results]
    │   │   └── PwBaseWorkChain<1127788> Finished [0] [5:results]
    │   │       └── PwCalculation<1132353> Finished [0]
    │   ├── extract_from_parameters<1141324> Finished [0]
    │   └── extract_from_trajectory<1141328> Finished [0]
    ├── scale_structure<1126744> Finished [0]
    └── QuantumEspressoCommonRelaxWorkChain<1126842> Finished [0] [2:convert_outputs]
        ├── PwRelaxWorkChain<1127198> Finished [0] [3:results]
        │   └── PwBaseWorkChain<1127858> Finished [0] [5:results]
        │       └── PwCalculation<1128415> Finished [0]
        ├── extract_from_parameters<1135412> Finished [0]
        └── extract_from_trajectory<1137745> Finished [0]
```

## Process Report (`verdi process report 1113585`)

```
2022-04-08 09:33:42 [363857 | REPORT]: [1113585|EquationOfStateWorkChain|run_init]: submitting `QuantumEspressoCommonRelaxWorkChain` for scale_factor `uuid: 49af2c94-44b7-4849-8e19-536a1729c155 (pk: 1113745) value: 0.94`
2022-04-08 09:34:31 [364354 | REPORT]:     [1114384|PwRelaxWorkChain|setup]: No change in volume possible for the provided base input parameters. Meta convergence is turned off.
2022-04-08 09:34:31 [364355 | REPORT]:     [1114384|PwRelaxWorkChain|setup]: Work chain will not run final SCF when `calculation` is set to `scf` for the relaxation `PwBaseWorkChain`.
2022-04-08 09:34:32 [364357 | REPORT]:     [1114384|PwRelaxWorkChain|run_relax]: launching PwBaseWorkChain<1115624>
2022-04-08 09:35:24 [364901 | REPORT]:       [1115624|PwBaseWorkChain|run_process]: launching PwCalculation<1117133> iteration #1
2022-04-08 09:39:04 [367429 | REPORT]:       [1115624|PwBaseWorkChain|results]: work chain completed after 1 iterations
2022-04-08 09:39:04 [367433 | REPORT]:       [1115624|PwBaseWorkChain|on_terminated]: remote folders will not be cleaned
2022-04-08 09:39:12 [367520 | REPORT]:     [1114384|PwRelaxWorkChain|results]: workchain completed after 1 iterations
2022-04-08 09:39:12 [367525 | REPORT]:     [1114384|PwRelaxWorkChain|on_terminated]: remote folders will not be cleaned
2022-04-08 09:39:31 [367777 | REPORT]:   [1114126|QuantumEspressoCommonRelaxWorkChain|inspect_workchain]: PwRelaxWorkChain<1114384> finished successfully.
2022-04-08 09:40:24 [368171 | REPORT]: [1113585|EquationOfStateWorkChain|run_eos]: submitting `QuantumEspressoCommonRelaxWorkChain` for scale_factor `0.96`
2022-04-08 09:40:27 [368199 | REPORT]: [1113585|EquationOfStateWorkChain|run_eos]: submitting `QuantumEspressoCommonRelaxWorkChain` for scale_factor `0.98`
2022-04-08 09:40:30 [368243 | REPORT]: [1113585|EquationOfStateWorkChain|run_eos]: submitting `QuantumEspressoCommonRelaxWorkChain` for scale_factor `1.0`
2022-04-08 09:40:31 [368254 | REPORT]: [1113585|EquationOfStateWorkChain|run_eos]: submitting `QuantumEspressoCommonRelaxWorkChain` for scale_factor `1.02`
2022-04-08 09:40:36 [368309 | REPORT]:     [1125394|PwRelaxWorkChain|setup]: No change in volume possible for the provided base input parameters. Meta convergence is turned off.
2022-04-08 09:40:36 [368310 | REPORT]:     [1125394|PwRelaxWorkChain|setup]: Work chain will not run final SCF when `calculation` is set to `scf` for the relaxation `PwBaseWorkChain`.
2022-04-08 09:40:36 [368319 | REPORT]:     [1125394|PwRelaxWorkChain|run_relax]: launching PwBaseWorkChain<1125456>
2022-04-08 09:40:59 [368543 | REPORT]:       [1125456|PwBaseWorkChain|run_process]: launching PwCalculation<1126000> iteration #1
2022-04-08 09:41:34 [368764 | REPORT]: [1113585|EquationOfStateWorkChain|run_eos]: submitting `QuantumEspressoCommonRelaxWorkChain` for scale_factor `1.04`
2022-04-08 09:41:44 [368818 | REPORT]: [1113585|EquationOfStateWorkChain|run_eos]: submitting `QuantumEspressoCommonRelaxWorkChain` for scale_factor `1.06`
2022-04-08 09:41:47 [368833 | REPORT]:     [1126816|PwRelaxWorkChain|setup]: No change in volume possible for the provided base input parameters. Meta convergence is turned off.
2022-04-08 09:41:47 [368834 | REPORT]:     [1126816|PwRelaxWorkChain|setup]: Work chain will not run final SCF when `calculation` is set to `scf` for the relaxation `PwBaseWorkChain`.
2022-04-08 09:41:48 [368835 | REPORT]:     [1126816|PwRelaxWorkChain|run_relax]: launching PwBaseWorkChain<1126890>
2022-04-08 09:41:59 [368979 | REPORT]:       [1126890|PwBaseWorkChain|run_process]: launching PwCalculation<1127194> iteration #1
2022-04-08 09:42:18 [369184 | REPORT]:     [1126831|PwRelaxWorkChain|setup]: No change in volume possible for the provided base input parameters. Meta convergence is turned off.
2022-04-08 09:42:18 [369185 | REPORT]:     [1126831|PwRelaxWorkChain|setup]: Work chain will not run final SCF when `calculation` is set to `scf` for the relaxation `PwBaseWorkChain`.
2022-04-08 09:42:18 [369191 | REPORT]:     [1126831|PwRelaxWorkChain|run_relax]: launching PwBaseWorkChain<1127788>
2022-04-08 09:42:21 [369217 | REPORT]:     [1127198|PwRelaxWorkChain|setup]: No change in volume possible for the provided base input parameters. Meta convergence is turned off.
2022-04-08 09:42:21 [369219 | REPORT]:     [1127198|PwRelaxWorkChain|setup]: Work chain will not run final SCF when `calculation` is set to `scf` for the relaxation `PwBaseWorkChain`.
2022-04-08 09:42:22 [369223 | REPORT]:     [1127198|PwRelaxWorkChain|run_relax]: launching PwBaseWorkChain<1127858>
2022-04-08 09:42:54 [369447 | REPORT]:       [1127858|PwBaseWorkChain|run_process]: launching PwCalculation<1128415> iteration #1
2022-04-08 09:45:20 [370709 | REPORT]:       [1125456|PwBaseWorkChain|results]: work chain completed after 1 iterations
2022-04-08 09:45:20 [370710 | REPORT]:       [1125456|PwBaseWorkChain|on_terminated]: remote folders will not be cleaned
2022-04-08 09:45:51 [370890 | REPORT]:       [1126890|PwBaseWorkChain|results]: work chain completed after 1 iterations
2022-04-08 09:45:51 [370891 | REPORT]:       [1126890|PwBaseWorkChain|on_terminated]: remote folders will not be cleaned
2022-04-08 09:45:53 [370919 | REPORT]:     [1125394|PwRelaxWorkChain|results]: workchain completed after 1 iterations
2022-04-08 09:45:54 [370921 | REPORT]:     [1125394|PwRelaxWorkChain|on_terminated]: remote folders will not be cleaned
2022-04-08 09:45:59 [370971 | REPORT]:     [1125552|PwRelaxWorkChain|setup]: No change in volume possible for the provided base input parameters. Meta convergence is turned off.
2022-04-08 09:45:59 [370972 | REPORT]:     [1125552|PwRelaxWorkChain|setup]: Work chain will not run final SCF when `calculation` is set to `scf` for the relaxation `PwBaseWorkChain`.
2022-04-08 09:45:59 [370976 | REPORT]:     [1125552|PwRelaxWorkChain|run_relax]: launching PwBaseWorkChain<1132127>
2022-04-08 09:45:59 [370978 | REPORT]:     [1125650|PwRelaxWorkChain|setup]: No change in volume possible for the provided base input parameters. Meta convergence is turned off.
2022-04-08 09:45:59 [370979 | REPORT]:     [1125650|PwRelaxWorkChain|setup]: Work chain will not run final SCF when `calculation` is set to `scf` for the relaxation `PwBaseWorkChain`.
2022-04-08 09:46:00 [370984 | REPORT]:     [1125650|PwRelaxWorkChain|run_relax]: launching PwBaseWorkChain<1132140>
2022-04-08 09:46:14 [371078 | REPORT]:       [1127788|PwBaseWorkChain|run_process]: launching PwCalculation<1132353> iteration #1
2022-04-08 09:46:27 [371150 | REPORT]:       [1132127|PwBaseWorkChain|run_process]: launching PwCalculation<1132581> iteration #1
2022-04-08 09:46:33 [371226 | REPORT]:       [1132140|PwBaseWorkChain|run_process]: launching PwCalculation<1132744> iteration #1
2022-04-08 09:46:40 [371304 | REPORT]:       [1127858|PwBaseWorkChain|results]: work chain completed after 1 iterations
2022-04-08 09:46:40 [371306 | REPORT]:       [1127858|PwBaseWorkChain|on_terminated]: remote folders will not be cleaned
2022-04-08 09:47:08 [371516 | REPORT]:   [1125214|QuantumEspressoCommonRelaxWorkChain|inspect_workchain]: PwRelaxWorkChain<1125394> finished successfully.
2022-04-08 09:47:30 [371697 | REPORT]:     [1126816|PwRelaxWorkChain|results]: workchain completed after 1 iterations
2022-04-08 09:47:30 [371701 | REPORT]:     [1126816|PwRelaxWorkChain|on_terminated]: remote folders will not be cleaned
2022-04-08 09:48:06 [372056 | REPORT]:     [1127198|PwRelaxWorkChain|results]: workchain completed after 1 iterations
2022-04-08 09:48:07 [372060 | REPORT]:     [1127198|PwRelaxWorkChain|on_terminated]: remote folders will not be cleaned
2022-04-08 09:49:37 [372707 | REPORT]:       [1132140|PwBaseWorkChain|results]: work chain completed after 1 iterations
2022-04-08 09:49:37 [372709 | REPORT]:       [1132140|PwBaseWorkChain|on_terminated]: remote folders will not be cleaned
2022-04-08 09:49:39 [372720 | REPORT]:   [1126842|QuantumEspressoCommonRelaxWorkChain|inspect_workchain]: PwRelaxWorkChain<1127198> finished successfully.
2022-04-08 09:50:13 [372936 | REPORT]:   [1125353|QuantumEspressoCommonRelaxWorkChain|inspect_workchain]: PwRelaxWorkChain<1126816> finished successfully.
2022-04-08 09:53:04 [374428 | REPORT]:       [1132127|PwBaseWorkChain|results]: work chain completed after 1 iterations
2022-04-08 09:53:05 [374434 | REPORT]:       [1132127|PwBaseWorkChain|on_terminated]: remote folders will not be cleaned
2022-04-08 09:54:43 [375759 | REPORT]:     [1125552|PwRelaxWorkChain|results]: workchain completed after 1 iterations
2022-04-08 09:54:43 [375762 | REPORT]:     [1125552|PwRelaxWorkChain|on_terminated]: remote folders will not be cleaned
2022-04-08 09:54:46 [375829 | REPORT]:     [1125650|PwRelaxWorkChain|results]: workchain completed after 1 iterations
2022-04-08 09:54:46 [375835 | REPORT]:     [1125650|PwRelaxWorkChain|on_terminated]: remote folders will not be cleaned
2022-04-08 09:54:51 [375936 | REPORT]:   [1125160|QuantumEspressoCommonRelaxWorkChain|inspect_workchain]: PwRelaxWorkChain<1125552> finished successfully.
2022-04-08 09:54:53 [375975 | REPORT]:       [1127788|PwBaseWorkChain|results]: work chain completed after 1 iterations
2022-04-08 09:54:53 [375978 | REPORT]:       [1127788|PwBaseWorkChain|on_terminated]: remote folders will not be cleaned
2022-04-08 09:54:53 [375989 | REPORT]:   [1125318|QuantumEspressoCommonRelaxWorkChain|inspect_workchain]: PwRelaxWorkChain<1125650> finished successfully.
2022-04-08 09:55:20 [376398 | REPORT]:     [1126831|PwRelaxWorkChain|results]: workchain completed after 1 iterations
2022-04-08 09:55:20 [376405 | REPORT]:     [1126831|PwRelaxWorkChain|on_terminated]: remote folders will not be cleaned
2022-04-08 09:55:20 [376406 | REPORT]:   [1126742|QuantumEspressoCommonRelaxWorkChain|inspect_workchain]: PwRelaxWorkChain<1126831> finished successfully.
2022-04-08 09:55:25 [376473 | REPORT]: [1113585|EquationOfStateWorkChain|inspect_eos]: Image 0: volume=56.47842671462601, total energy=-7817.0310157283
2022-04-08 09:55:25 [376474 | REPORT]: [1113585|EquationOfStateWorkChain|inspect_eos]: Image 1: volume=57.68009536812749, total energy=-7817.0446343454
2022-04-08 09:55:25 [376475 | REPORT]: [1113585|EquationOfStateWorkChain|inspect_eos]: Image 2: volume=58.88176402163311, total energy=-7817.0523302689
2022-04-08 09:55:25 [376477 | REPORT]: [1113585|EquationOfStateWorkChain|inspect_eos]: Image 3: volume=60.08343267513449, total energy=-7817.0548439205
2022-04-08 09:55:25 [376479 | REPORT]: [1113585|EquationOfStateWorkChain|inspect_eos]: Image 4: volume=61.28510132863885, total energy=-7817.0528233392
2022-04-08 09:55:25 [376481 | REPORT]: [1113585|EquationOfStateWorkChain|inspect_eos]: Image 5: volume=62.48676998213998, total energy=-7817.0468375151
2022-04-08 09:55:25 [376482 | REPORT]: [1113585|EquationOfStateWorkChain|inspect_eos]: Image 6: volume=63.68843863564458, total energy=-7817.037385505
```

## Node Info (`verdi node show d4d3af69-e1d4-47ce-8f4e-b28cde53c0b3`)

```
Property     Value
-----------  ------------------------------------
type         EquationOfStateWorkChain
state        Finished [0]
pk           1113585
uuid         d4d3af69-e1d4-47ce-8f4e-b28cde53c0b3
label
description
ctime        2022-04-08 09:33:25.123591+00:00
mtime        2022-04-08 09:55:26.070747+00:00

Inputs                PK       Type
--------------------  -------  -------------
sub_process
    base
        pw
            settings  1113578  Dict
scale_count           1113580  Int
scale_increment       1113583  Float
structure             1077636  StructureData

Outputs         PK       Type
--------------  -------  -------------
structures
    6           1126829  StructureData
    0           1113871  StructureData
    1           1125139  StructureData
    4           1125324  StructureData
    2           1125193  StructureData
    5           1126732  StructureData
    3           1125247  StructureData
total_energies
    4           1135828  Float
    0           1124121  Float
    3           1140898  Float
    1           1140781  Float
    2           1133985  Float
    5           1141326  Float
    6           1135501  Float

Called         PK  Type
--------  -------  -----------------------------------
CALL      1113746  scale_structure
CALL      1114126  QuantumEspressoCommonRelaxWorkChain
CALL      1125111  scale_structure
CALL      1125160  QuantumEspressoCommonRelaxWorkChain
CALL      1125162  scale_structure
CALL      1125214  QuantumEspressoCommonRelaxWorkChain
CALL      1125219  scale_structure
CALL      1125318  QuantumEspressoCommonRelaxWorkChain
CALL      1125322  scale_structure
CALL      1125353  QuantumEspressoCommonRelaxWorkChain
CALL      1125357  scale_structure
CALL      1126742  QuantumEspressoCommonRelaxWorkChain
CALL      1126744  scale_structure
CALL      1126842  QuantumEspressoCommonRelaxWorkChain

Log messages
----------------------------------------------
There are 14 log messages for this calculation
Run 'verdi process report 1113585' to see them
```
