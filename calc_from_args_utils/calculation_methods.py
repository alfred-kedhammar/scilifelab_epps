#!/usr/bin/env python
import logging
from argparse import Namespace

import numpy as np
import pandas as pd
import tabulate
from genologics.entities import Process

from calc_from_args_utils.udf_arg_methods import (
    fetch_from_arg,
    get_UDF_source,
    get_UDF_source_name,
)
from epp_utils import formula, udf_tools

DESC = """This file contains the method functions for a UDF-agnostic script."""


def volume_to_use(process: Process, args: Namespace):
    """Calculate how much volume to use based on a target amount.

    Uses target amount, concentration, conc.units and size.
    """
    art_tuples = udf_tools.get_art_tuples(process)

    for art_tuple in art_tuples:
        try:
            # Explicate current working input-output tuple
            art_in = art_tuple[0]["uri"]
            art_out = art_tuple[1]["uri"]
            logging.info("")
            logging.info(
                f"Processing input '{art_in.name}' -> output '{art_out.name}'..."
            )

            # Get info specified by script arguments
            size_bp = fetch_from_arg(art_tuple, args.size_in, process)
            input_conc = fetch_from_arg(art_tuple, args.conc_in, process)
            input_vol = fetch_from_arg(art_tuple, args.vol_in, process)
            output_amt = fetch_from_arg(art_tuple, args.amt_out, process)
            if args.conc_units_in:
                input_conc_units = fetch_from_arg(
                    art_tuple, args.conc_units_in, process
                )
                assert input_conc_units in [
                    "ng/ul",
                    "nM",
                ], f"Unsupported conc. units '{input_conc_units}'"
            else:
                if "ng" in args.conc_in["udf"]:
                    input_conc_units = "nM"
                elif "nM" in args.conc_in["udf"]:
                    input_conc_units = "nM"
                else:
                    raise AssertionError(
                        f"No concentration units can inferred from {args.conc_in}."
                    )

            # Calculate required volume
            if input_conc_units == "nM":
                vol_required = output_amt / input_conc
            elif input_conc_units == "ng/ul":
                vol_required = min(
                    formula.fmol_to_ng(output_amt, size_bp) / input_conc, input_vol
                )
            logging.info(
                f"Calculating required volume: {output_amt} fmol of {input_conc} {input_conc_units} at {size_bp} bp -> {vol_required:.2f} ul."
            )

            # Adress case of volume depletion
            if vol_required > input_vol:
                logging.warning(
                    f"Volume required ({vol_required:.2f} ul) is greater than the available input '{args.vol_in['udf']}': {input_vol:.2f}."
                )
                logging.warning("Using all available volume.")
                vol_to_take = input_vol

                new_output_amt = vol_to_take * input_conc
                logging.info(
                    f"Updating amount used -> '{args.amt_out['udf']}': {output_amt} -> {new_output_amt:.2f}"
                )
            else:
                vol_to_take = vol_required

            logging.info(f"Determined volume to take -> {vol_to_take:.2f} ul.")

            # Update UDFs
            udf_tools.put(
                target=get_UDF_source(art_tuple, args.vol_out, process),
                target_udf=args.vol_out["udf"],
                val=round(vol_to_take, 2),
            )
            logging.info(
                f"Assigned UDF '{args.vol_out['udf']}': {vol_to_take:.2f} for {args.vol_out['source']} '{get_UDF_source_name(art_tuple, args.vol_out, process)}'."
            )
            if vol_required > input_vol:
                udf_tools.put(
                    target=get_UDF_source(art_tuple, args.amt_out, process),
                    target_udf=args.amt_out["udf"],
                    val=round(new_output_amt, 2),
                )
                logging.warning(
                    f"Changed UDF '{args.amt_out['udf']}': {output_amt} -> {new_output_amt:.2f} for {args.amt_out['source']} '{get_UDF_source_name(art_tuple, args.amt_out, process)}'."
                )
        except AssertionError as e:
            logging.error(str(e), exc_info=True)
            logging.warning("Skipping.")
            continue


def equimolar_pooling(process: Process, args: Namespace):
    """Perform equimolar pooling based on a target molar amount or volume."""

    step_tuples = udf_tools.get_art_tuples(process)

    pools = [art for art in process.all_outputs() if art.type == "Analyte"]
    pools.sort(key=lambda pool: pool.name)

    # Iterate across every pool
    for pool in pools:
        logging.info("")
        logging.info(f"Processing pool '{pool.name}'...")

        # Subset tuples of current pool
        pool_tuples = [
            art_tuple for art_tuple in step_tuples if art_tuple[1]["uri"].id == pool.id
        ]

        # Start collecting dataframe rows
        pool_data_rows = []

        # Iterate across all pool inputs
        for art_tuple in pool_tuples:
            cols: dict[str, float | str] = {}

            art_in = art_tuple[0]["uri"]
            art_out = art_tuple[1]["uri"]
            logging.info("")
            logging.info(
                f"Processing input '{art_in.name}' -> output '{art_out.name}'..."
            )

            # Get info specified by script arguments
            cols["size_bp"] = fetch_from_arg(art_tuple, args.size_in, process)
            cols["input_conc"] = fetch_from_arg(art_tuple, args.conc_in, process)
            cols["input_vol"] = fetch_from_arg(art_tuple, args.vol_in, process)
            if args.conc_units_in:
                cols["input_conc_units"] = str(
                    fetch_from_arg(art_tuple, args.conc_units_in, process)
                )
                assert (
                    cols["input_conc_units"] in ["ng/ul", "nM"]
                ), f'Unsupported conc. units "{cols["input_conc_units"]}" for art {art_in.name}'
            else:
                # Infer concentration unit
                if "ng/ul" in args.conc_in["udf"]:
                    cols["input_conc_units"] = "ng/ul"
                elif "nM" in args.conc_in["udf"]:
                    cols["input_conc_units"] = "nM"
                else:
                    raise AssertionError(
                        f"Can't infer units from '{args.conc_in['udf']}' for {art_out.name}."
                    )
                logging.info(
                    f"Inferred unit of UDF '{args.conc_in['udf']}': {cols['input_conc_units']}."
                )

            # Infer amount unit
            if "fmol" in args.amt_out["udf"]:
                cols["output_amt_unit"] = "fmol"
            elif "ng" in args.amt_out["udf"]:
                cols["output_amt_unit"] = "ng"
            else:
                raise AssertionError(
                    f"Can't infer units from '{args.amt_out['udf']}' for art {art_out.name}"
                )
            logging.info(
                f"Inferred unit of UDF '{args.amt_out['udf']}': {cols['output_amt_unit']}."
            )

            pool_data_rows.append(cols)

        df_pool = pd.DataFrame(pool_data_rows)
        df_pool.index = [art_tuple[0]["uri"].name for art_tuple in pool_tuples]

        logging.info(
            f"Collected data for pool '{pool.name}':\n{tabulate.tabulate(df_pool, headers=df_pool.columns)}"
        )

        assert (
            df_pool.output_amt_unit.unique().size == 1
        ), "Inconsistent output amount units."

        # Get a column with consistent concentration units
        df_pool["input_conc_nM"] = df_pool.apply(
            lambda x: x["input_conc"]
            if x["input_conc_units"] == "nM"
            else formula.ng_ul_to_nM(x["input_conc"], x["size_bp"]),
            axis=1,
        )

        # Get concentrations proportions between samples
        df_pool["prop_nM"] = df_pool.input_conc_nM / sum(df_pool.input_conc_nM)
        # Inverse the concentration proportions to get the volume proportions for equimolar pooling
        df_pool["prop_nM_inv"] = (1 / df_pool["prop_nM"]) / sum(1 / df_pool["prop_nM"])

        # Get target parameters for pool
        pool_target_amt_fmol: float | None = fetch_from_arg(
            pool_tuples[0], args.amt_out, process, on_fail=None
        )
        pool_target_vol: float | None = fetch_from_arg(
            pool_tuples[0], args.vol_out, process, on_fail=None
        )

        # If amount is specified, use for calculations and ignore target vol
        if pool_target_amt_fmol:
            logging.info(
                f"Basing calculations on pool target amount '{args.amt_out['udf']}': {pool_target_amt_fmol:.1f} fmol."
            )
            sample_target_amt_fmol = pool_target_amt_fmol / len(df_pool)

            # Calculate transfer volumes
            df_pool["transfer_vol_ul"] = np.minimum(
                sample_target_amt_fmol / df_pool.input_conc_nM, df_pool.input_vol
            )

        # If amount is omitted but not target volume, use target volume for calculations
        elif pool_target_vol:
            logging.info(
                f"Basing calculations on pool target volume '{args.vol_out['udf']}': {pool_target_vol:.1f} ul."
            )

            # Apply molar proportions to target volume to get transfer amounts
            df_pool["transfer_vol_ul"] = np.minimum(
                pool_target_vol * df_pool.prop_nM_inv, df_pool.input_vol
            )

        else:
            raise AssertionError(
                f"No target amount or volume specified for pool '{pool.name}'"
            )

        # Warn about sample depletion
        depleted_samples = df_pool[df_pool.input_vol == df_pool.transfer_vol_ul].index
        for depleted_sample in depleted_samples:
            logging.warning(f"Volume depletion in sample: {depleted_sample}.")

        # Calculate final amounts and volumes
        df_pool["transfer_amt_fmol"] = df_pool.transfer_vol_ul * df_pool.input_conc_nM
        pool_vol = sum(df_pool.transfer_vol_ul)
        pool_amt_fmol = sum(df_pool.transfer_amt_fmol)
        # Calculate the average fragment size of the pool, weighted by the transfer amounts
        pool_size = round(
            np.average(df_pool["size_bp"], weights=df_pool["transfer_amt_fmol"])
        )

        # Evaluate target fraction to 1 decimal
        df_pool["molar_percentage"] = (
            df_pool.transfer_amt_fmol / pool_amt_fmol * 100
        ).round(1)

        logging_str = "\n".join(
            [
                f"Finalized calculations for pool '{pool.name}':",
                f"Target amount: {pool_target_amt_fmol:.1f} fmol"
                if pool_target_amt_fmol
                else "Target amount: None",
                f"Target volume: {pool_target_vol:.1f} ul"
                if pool_target_vol
                else "Target volume: None",
                f"Final amount: {pool_amt_fmol:.1f} fmol",
                f"Total volume: {pool_vol:.1f} ul",
                f"Average size: {pool_size} bp",
                tabulate.tabulate(df_pool, headers=df_pool.columns),
            ]
        )
        logging.info(logging_str)

        # Flag samples pooled off-target by more than a promille
        non_equimolar_samples = df_pool[
            df_pool.molar_percentage != round(100 / len(df_pool), 1)
        ].index
        for non_equimolar_sample in non_equimolar_samples:
            logging.warning(
                f"Non-equimolar pooling  of sample: {non_equimolar_sample}."
            )

        # Update UDFs
        pool.udf[args.amt_out["udf"]] = round(pool_amt_fmol, 2)
        logging.info(
            f"Assigned '{pool.name}' UDF '{args.amt_out['udf']}': {pool_amt_fmol:.2f}"
        )
        pool.udf[args.vol_out["udf"]] = round(pool_vol, 1)
        logging.info(
            f"Assigned '{pool.name}' UDF '{args.vol_out['udf']}': {pool_vol:.1f}"
        )
        if args.size_out:
            pool.udf[args.size_out["udf"]] = round(pool_size, 1)
            logging.info(
                f"Assigned '{pool.name}' UDF '{args.size_out['udf']}': {pool_size:.1f}"
            )
        pool.put()


def amount(process: Process, args: Namespace):
    """Calculate amount.

    Uses volume, concentration, conc.units and size.
    """
    art_tuples = udf_tools.get_art_tuples(process)

    for art_tuple in art_tuples:
        try:
            art_in = art_tuple[0]["uri"]
            art_out = art_tuple[1]["uri"]
            logging.info("")
            logging.info(
                f"Processing input '{art_in.name}' -> output '{art_out.name}'..."
            )

            # Get info specified by script arguments
            size_bp = fetch_from_arg(art_tuple, args.size_in, process)
            input_conc = fetch_from_arg(art_tuple, args.conc_in, process)
            input_vol = fetch_from_arg(art_tuple, args.vol_in, process)
            if args.conc_units_in:
                input_conc_units = str(
                    fetch_from_arg(art_tuple, args.conc_units_in, process)
                )
                assert input_conc_units in [
                    "ng/ul",
                    "nM",
                ], f'Unsupported conc. units "{input_conc_units}" for art {art_in.name}'
            else:
                # Infer concentration unit
                if "ng/ul" in args.conc_in["udf"]:
                    input_conc_units = "ng/ul"
                elif "nM" in args.conc_in["udf"]:
                    input_conc_units = "nM"
                else:
                    raise AssertionError(
                        f"Can't infer units from '{args.conc_in['udf']}' for {art_out.name}."
                    )
                logging.info(
                    f"Inferred unit of UDF '{args.conc_in['udf']}': {input_conc_units}."
                )

            # Infer amount unit
            if "fmol" in args.amt_out["udf"]:
                output_amt_unit = "fmol"
            elif "ng" in args.amt_out["udf"]:
                output_amt_unit = "ng"
            else:
                raise AssertionError(
                    f"Can't infer units from '{args.amt_out['udf']}' for art {art_out.name}"
                )
            logging.info(
                f"Inferred unit of UDF '{args.amt_out['udf']}': {output_amt_unit}."
            )

            # Calculate amount
            if input_conc_units == "nM":
                if output_amt_unit == "fmol":
                    output_amt = input_vol * input_conc
                elif output_amt_unit == "ng":
                    output_amt = formula.fmol_to_ng(input_vol * input_conc, size_bp)
            elif input_conc_units == "ng/ul":
                if output_amt_unit == "fmol":
                    output_amt = formula.ng_to_fmol(input_vol * input_conc, size_bp)
                elif output_amt_unit == "ng":
                    output_amt = input_vol * input_conc
            logging.info(
                f"Calculating amount: {input_vol} ul of {input_conc} {input_conc_units} at {size_bp} bp -> {output_amt:.2f} {output_amt_unit}"
            )

            # Update amount UDF of output artifact
            udf_tools.put(
                target=get_UDF_source(art_tuple, args.amt_out, process),
                target_udf=args.amt_out["udf"],
                val=round(output_amt, 2),
            )
            logging.info(
                f"Assigned UDF '{args.amt_out['udf']}': {output_amt:.2f} for {args.amt_out['source']} '{get_UDF_source_name(art_tuple, args.amt_out, process)}'."
            )

        except AssertionError as e:
            logging.error(str(e), exc_info=True)
            logging.warning("Skipping.")
            continue
