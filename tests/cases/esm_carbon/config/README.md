# ICON-ESM R02B08 with carbon cycle

Coupled ICON-ESM experiment at R02B08 on CSCS Alps (santis), translated from
`jcp/run_scripts/santis/jcp_r2b8_icon4py.run`:

| Component | Target | Ranks |
|-----------|--------|-------|
| atmosphere (AES) and land (JSBACH) | GPU, dynamical core through ICON4Py (`icon4py_mode` substitute) | 4 per node |
| ocean and biogeochemistry (HAMOCC) | CPU | 100 per node |

Atmosphere and ocean exchange fields through YAC every 12 minutes (`ICON/coupling.yaml`).
The workflow runs on 16 nodes with monthly cycles from 2020-01-01 to 2020-06-01; each cycle
restarts from the atmosphere and ocean restarts of the previous one.

## Running

1. Adapt `sirocco_vars.yaml` (account, Python environment, ICON builds, input and cache directories).
2. `sirocco verify tests/cases/esm_carbon/config`
3. `sirocco start tests/cases/esm_carbon/config`

`runtime/main.sh` adds a hang watchdog: if the job log does not grow for
`WATCHDOG_TIMEOUT` seconds (`WATCHDOG_INIT_TIMEOUT` before the first time step), it collects
node, GPU, network and gdb diagnostics in `diag.<jobid>/` and aborts the model step.

## Known issue: crash 38 steps after a restart

Status 2026-10-16: open. Every cycle after the first one crashes; a single cycle runs to completion.

### Observations

Runs used for the analysis (ICON `icon-mpim` commit `bea16a4c5d`, builds
`build-santis_icon4py_double` and `build-santis_cpu_double_py_substitute`, uenv `icon/26.7:v1`):

| Run | Setup | Job | Result |
|-----|-------|-----|--------|
| `sirocco_runs/esm_carbon_p2d` | one 2-day cycle, no restart | 937543 | completed, 8640 steps |
| `sirocco_runs/esm_carbon_smoke`, cycle 1 | 1-day cycle, cold start | 935435 | completed, restart written |
| `sirocco_runs/esm_carbon_smoke`, cycle 2 | restart at 2020-01-02 | 935436 | crashed at step 4360 |
| `sirocco_runs/esm_carbon_smoke`, cycle 2 | same, other nodes | 937540 | crashed at step 4360 |

All ranks stop with

```
FINISH PE: 0 mo_nh_stepping: set_ndyn_substeps: Number of dynamics substeps in domain 1 went out of bounds: -2147483644
```

The comparison of the per-substep `MAXABS VN, W` output (`msg_level = 10`) of the restarted
cycle with the continuous 2-day run gives:

1. Steps 4321 to 4357 are bit-identical, so the restart itself reproduces the continuous run.
2. In step 4358 (model time 2020-01-02 00:12:40) only, the normal wind at the lowest model level
   (level 90) is far out of range at the first substep (5.9e16 m/s instead of 206.6 m/s) and grows
   to 1e69 m/s; the vertical wind follows (up to 1e66 m/s at levels 85 to 91). A physical
   instability cannot grow this fast.
3. From step 4359 on, the maximum winds are again bit-identical to the continuous run. The
   corrupted values therefore did not enter the prognostic state that is carried forward.
4. The values are bit-identical in both crashed jobs, which ran on different nodes. The defect
   is deterministic; a race condition or a hardware fault is unlikely.
5. Step 4357 is the first step after the restart with a full radiation computation
   (`dt_rad = PT12M`) and the first regular YAC exchange (`coupling_period: PT12M`) at 00:12.
6. The `cxil_map: write error` messages of some ocean ranks during initialization also appear in
   the successful continuous run and are unrelated.

### Sequence of the crash

1. In step 4358 ICON4Py returns an extreme maximum vertical CFL number
   (`max_vcfl_dyn`, passed back through `max_vcfl_size1_array`).
2. `max_vcfl_dyn` accumulates as a maximum until `set_ndyn_substeps` evaluates and resets it,
   which happens every `cfl_monitoring_freq = 5` steps, here at step 4360. Although the state is
   healthy again in step 4360, the stored spike enters watch mode
   (`Maximum vertical CFL number in domain 1:*******`).
3. `set_ndyn_substeps` (`src/atm_dyn_iconam/mo_nh_stepping.f90:3733`) computes
   `nsubs_add = MAX(1, NINT(REAL(ndyn_substeps_var)*(mvcfl-thresh1_vcfl)/thresh1_vcfl))`.
   For the huge `mvcfl`, `NINT` saturates at `HUGE(0) = 2147483647`.
4. Line 3734, `MIN(ndyn_substeps_var+nsubs_add, ndyn_substeps_max+ndyn_substeps_enh)`, adds
   `5 + 2147483647`, which wraps around to `-2147483644`. The negative value passes the `MIN`
   cap and triggers the out-of-bounds `FINISH`. The reported number matches this overflow exactly.

### What we consider buggy in ICON

1. **Transient corruption in the Fortran/OpenACC code in the first radiation and coupling step
   after a restart (root cause, not yet located).** The defect is deterministic and limited to one
   time step after the restart. In our assessment it does not originate in ICON4Py; the
   dynamical core only receives and propagates the corrupted input. Candidates, in
   order of likelihood:
   - a field that is read from the restart file into host memory but never copied to the device
     (missing `!$ACC UPDATE DEVICE`), and first used on the GPU at the first radiation or coupling
     step. A stale device copy is deterministic, and if the field is recomputed on the device in
     the following step, this also explains why the state is correct again from step 4359 on;
   - lowest-level surface coupling in AES (vertical diffusion `tmx` with ocean surface velocities
     and surface fluxes received from YAC), since the corruption starts at level 90; the
     physics-to-dynamics update of `vn` from `ua_phy`/`va_phy`
     (`src/atm_phy_aes/mo_interface_iconam_aes.f90:456`) acts on all levels;
   - the radiation fluxes of the first full radiation call after the restart, if a radiation
     input is not restored on the device.
2. **Integer overflow in `set_ndyn_substeps` (certain).** `NINT` of an unbounded CFL number and
   the addition before `MIN` can overflow, so the cap `ndyn_substeps_max` (`ndyn_substeps + 7`) is
   not effective. The error message then reports a meaningless negative number instead of an
   unstable CFL number. A non-finite or extreme `mvcfl` should be detected (e.g. `IEEE_IS_FINITE`
   and a plausibility limit) and either be capped before `NINT` or reported as a numerical
   instability.
3. **CFL output format (minor).** `'(a,i3,a,f7.4)'` prints `*******` for CFL numbers of 1000 and
   above, so the log does not show the actual value. An `es` format would.
4. **Hard-coded domain index in the ICON4Py interface (minor, latent).** `icon4py_solve_nh_substitute`
   passes `ndyn_substeps_var(1)` instead of the value of the current domain. This is irrelevant for
   this single-domain setup but wrong for nested domains.

### Next steps

- Compare the device and host copies (`!$ACC UPDATE HOST`) of the AES restart and coupling
  fields directly after reading the restart, and in step 4357, to find a stale device copy.
- Shift `dt_rad` or the coupling period by one step to tell whether radiation or coupling triggers
  the corruption.
- Restart the continuous run at a different date (e.g. 2020-01-03) and check whether the
  corruption again appears in the first radiation and coupling step after the restart.
- Use `msg_level >= 13` to obtain the rank that holds the maximum, and dump `vn`, `ddt_vn_phy`
  and the coupling fields of that rank in steps 4357 and 4358.
- Make `set_ndyn_substeps` overflow-safe. This does not fix the root cause but would show
  whether the model survives the transient spike, as the state suggests.
