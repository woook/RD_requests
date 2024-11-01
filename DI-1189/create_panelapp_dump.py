import argparse
import json
import pandas as pd
import sys

from collections import defaultdict
from panelapp.Panelapp import Panel
from panelapp import queries


def parse_args() -> argparse.Namespace:
    """
    Parse command line arguments

    Returns
    -------
    args : Namespace
        Namespace of passed command line argument inputs
    """
    parser = argparse.ArgumentParser(
        description="Information required to create PanelApp JSON"
    )

    parser.add_argument(
        "-g",
        "--genepanels",
        type=str,
        help=(
            "Genepanels file to be used to keep only specific panels "
            "(optional). Required if --extra_panels is given"
        ),
    )

    parser.add_argument(
        "-p",
        "--extra_panels",
        type=str,
        help=(
            "Extra panel IDs to retain (optional, requires --genepanels to "
            "be given)"
        ),
    )

    parser.add_argument(
        "-o",
        "--outfile_name",
        required=True,
        type=str,
        help="Name for output PanelApp JSON",
    )

    return parser.parse_args()


def read_in_genepanels(genepanels_file):
    """
    Read in genepanels TSV file given as command line argument which specifies
    the panels we test for

    Parameters
    ----------
    file_name : str
        Name of the genepanels file to read in

    Returns
    -------
    genepanels_df : pd.DataFrame
        the genepanels file as a pandas df
    """
    genepanels_df = pd.read_csv(
        genepanels_file,
        sep="\t",
        header=None,
        names=["panel_name", "panel_version", "gene_id", "panel_id"],
    )

    # Convert panel IDs to int (because they are read in as floats by default)
    genepanels_df["panel_id"] = pd.to_numeric(
        genepanels_df["panel_id"]
    ).astype("Int64")

    return genepanels_df


def get_unique_required_panel_ids(genepanels_df):
    """
    Get list of unique panel IDs to retain from input genepanels file

    Parameters
    ----------
    genepanels_df : pd.DataFrame
        the genepanels file as a pandas df

    Returns
    -------
    unique_panel_ids : list
        a list of unique panel IDs as strings
    """
    unique_panel_ids = list(
        map(str, list(genepanels_df["panel_id"].dropna().unique()))
    )
    assert unique_panel_ids, "No panel IDs found in the genepanels file"
    print(
        f"Found {len(unique_panel_ids)} unique panel IDs"
        " to keep from genepanels file provided."
    )

    return unique_panel_ids


def add_additional_panel_ids(unique_panel_ids, extra_panel_ids):
    """
    Add any additional panels given as CLI input '--extra_panels'
    to the list of panel IDs to keep

    Parameters
    ----------
    unique_panel_ids : list
        list of panel IDs to keep, each as string
    extra_panel_ids : str
        string of panel IDs, comma separated

    Returns
    -------
    unique_panel_ids : list
        panel IDs with any extra panel IDs added
    """
    if extra_panel_ids:
        print("Adding extra panel IDs to the list of panel IDs to keep:")
        extra_panel_ids = [
            panel.strip() for panel in extra_panel_ids.split(",")
        ]
        print("\t" + "\n\t".join(extra_panel_ids))
        unique_panel_ids.extend(extra_panel_ids)

        print(f"Retaining {len(unique_panel_ids)} panel IDs in total.")

    return unique_panel_ids


def _clean_val(val):
    """
    Deal with empty strings and lists returned from PanelApp.

    Parameters
    ----------
    val : str/list
        value to be cleaned

    Returns
    -------
    val : str/list
        cleaned value
    """
    if isinstance(val, str):
        return val.strip() if val.strip() else None
    elif isinstance(val, list):
        return ",".join([v for v in val]) if val else None
    else:
        return val


def _add_gene_info(panel: Panel, info_dict: dict) -> dict:
    """
    Iterate over every gene in the panel and retrieve info, add to info_dict

    Parameters
    ----------
    panel : Panel
        PanelApp data for one panel
    info_dict : dict
        dict to hold info about that panel

    Returns
    -------
    info_dict : dict
        dict with added info about the panel
    """
    gene_data = {}

    # fetching all genes information from PanelApp Panel
    for gene in panel.data.get("genes", []):
        hgnc_id = gene.get("gene_data", {}).get("hgnc_id")
        if hgnc_id:
            gene_data[hgnc_id] = gene

    # fetching all confidence 3 genes
    for gene in panel.genes.get("3", []):
        hgnc_id = gene.get("hgnc_id")
        if not hgnc_id:
            print(f"Skipping {gene}. No HGNC id found.")
            continue

        gene_info = gene_data.get(hgnc_id, {})
        gene_dict = {
            "transcript": _clean_val(gene_info.get("transcript")),
            "hgnc_id": hgnc_id,
            "confidence_level": gene_info.get("confidence_level"),
            "mode_of_inheritance": _clean_val(
                gene_info.get("mode_of_inheritance")
            ),
            "mode_of_pathogenicity": _clean_val(
                gene_info.get("mode_of_pathogenicity")
            ),
            "penetrance": _clean_val(gene_info.get("penetrance")),
            "gene_justification": "PanelApp",
            "transcript_justification": "PanelApp",
            "alias_symbols": _clean_val(
                gene_info["gene_data"].get("alias", None)
            ),
            "gene_symbol": gene_info["gene_data"].get("gene_symbol"),
        }

        if gene_dict not in info_dict["genes"]:
            info_dict["genes"].append(gene_dict)

    return info_dict


def _add_region_info(panel: Panel, info_dict: dict):
    """
    Iterate over every region in the panel and retrieve the data

    Parameters
    ----------
    panel : Panel
        PanelApp data for one panel
    info_dict : dict
        holds some info about that panel

    Returns
    -------
    info_dict : dict
        dict with info about a panel
    """
    if panel.data.get("regions"):
        for region in panel.data["regions"]:
            # only add confidence level 3 regions
            if region.get("confidence_level") == "3":
                # define start and end coordinates grch37
                if not region.get("grch37_coordinates"):
                    start_37, end_37 = None, None
                else:
                    start_37, end_37 = region.get("grch37_coordinates")

                # define start and end coordinates grch38
                if not region.get("grch38_coordinates"):
                    start_38, end_38 = None, None
                else:
                    start_38, end_38 = region.get("grch38_coordinates")

                region_dict = {
                    "confidence_level": region.get("confidence_level"),
                    "mode_of_inheritance": _clean_val(
                        region.get("mode_of_inheritance")
                    ),
                    "mode_of_pathogenicity": _clean_val(
                        region.get("mode_of_pathogenicity")
                    ),
                    "penetrance": _clean_val(region.get("penetrance")),
                    "name": region.get("verbose_name"),
                    "chrom": region.get("chromosome"),
                    "start_37": start_37,
                    "end_37": end_37,
                    "start_38": start_38,
                    "end_38": end_38,
                    "type": "CNV",
                    "variant_type": _clean_val(region.get("type_of_variants")),
                    "required_overlap": _clean_val(
                        region.get("required_overlap_percentage")
                    ),
                    "haploinsufficiency": _clean_val(
                        region.get("haploinsufficiency_score")
                    ),
                    "triplosensitivity": _clean_val(
                        region.get("triplosensitivity_score")
                    ),
                    "justification": "PanelApp",
                }
                if region_dict not in info_dict["regions"]:
                    info_dict["regions"].append(region_dict)

    return info_dict


def _parse_single_pa_panel(panel: Panel) -> dict:
    """
    Parse output of all-panel fetching function, which is a Panel object

    Parameters
    ----------
    panel : Panel
        PanelApp data for one panel

    Returns
    -------
    info_dict : dict
        dict with info about the panel
    """
    attributes = ["name", "version", "id"]  # List of attributes to check

    # Check if all attributes exist in the object
    if not all(hasattr(panel, attr) for attr in attributes):
        print(f"One or more required attributes for panel {panel} are missing")

    info_dict = {
        "panel_source": "PanelApp",
        "panel_name": panel.name,
        "external_id": panel.id,
        "panel_version": panel.version,
        "genes": [],
        "regions": [],
    }

    _add_gene_info(panel, info_dict)
    _add_region_info(panel, info_dict)

    return info_dict


def parse_specified_pa_panels(panel_ids=None) -> list:
    """
    Parse all panels. If a list of panel IDs is given, keep only the
    specified panels

    Parameters
    ----------
    panel_ids : list, optional
        list of strings, each representing a panel ID to retain

    Returns
    -------
    parsed_data : list
        list of dicts, each with info about a panel
    """
    parsed_data = []
    all_panels: dict[int, Panel] = queries.get_all_signedoff_panels()

    for panel_id, panel in all_panels.items():
        panel_data = _parse_single_pa_panel(panel)
        if not panel_data:
            print(f"Parsing failed for panel ID {panel_id}")
        parsed_data.append(panel_data)

    if panel_ids:
        parsed_data = [
            panel for panel in parsed_data if panel["external_id"] in panel_ids
        ]

    print(f"\nData parsing complete. {len(parsed_data)} panels retained:")
    panel_names = "\t" + "\n\t".join(
        [panel["panel_name"] for panel in parsed_data]
    )
    print(panel_names)

    return parsed_data


def find_duplicate_genes_or_regions(parsed_data):
    """
    Find any duplicate genes or regions within a panel

    Parameters
    ----------
    parsed_data : list
        list of dicts, each with info on a PanelApp panel

    Returns
    -------
    duplicates : dict
        dict with info on duplicates
    Example output for a duplicate region found:
    {
        'Paediatric disorders': {
            'regions': {
                '16p12.2 recurrent region (distal)(includes OTOA) Loss': [
                    {
                        'confidence_level': '3',
                        'mode_of_inheritance': 'Other',
                        'mode_of_pathogenicity': None,
                        'penetrance': None,
                        'name': (
                            '16p12.2 recurrent region (distal)(includes OTOA)'
                            ' Loss'
                        ),
                        'chrom': '16',
                        'start_37': None,
                        'end_37': None,
                        'start_38': 21558792,
                        'end_38': 21729102,
                        'type': 'CNV',
                        'variant_type': 'cnv_loss',
                        'required_overlap': 60,
                        'haploinsufficiency': '30',
                        'triplosensitivity': None,
                        'justification': 'PanelApp'
                    },
                    {
                        'confidence_level': '3',
                        'mode_of_inheritance': (
                            'MONOALLELIC, autosomal or pseudoautosomal, '
                            'imprinted status unknown'
                        ),
                        'mode_of_pathogenicity': None,
                        'penetrance': None,
                        'name': (
                            '16p12.2 recurrent region (distal)(includes OTOA)'
                            ' Loss'
                        ),
                        'chrom': '16',
                        'start_37': None,
                        'end_37': None,
                        'start_38': 21558792,
                        'end_38': 21729102,
                        'type': 'CNV',
                        'variant_type': 'cnv_loss',
                        'required_overlap': 60,
                        'haploinsufficiency': '30',
                        'triplosensitivity': None,
                        'justification': 'PanelApp'
                    }
                ]
            }
        }
    }
    """
    # Convert our list of dicts to nested dict per panel, with nested
    # key for each gene symbol or region name so we can find any dups
    duplicates = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    for panel in parsed_data:
        panel_name = panel["panel_name"]

        genes = defaultdict(list)
        for gene in panel["genes"]:
            genes[gene["gene_symbol"]].append(gene)
        for gene_name, gene_list in genes.items():
            if len(gene_list) > 1:
                duplicates[panel_name]["genes"][gene_name] = gene_list

        regions = defaultdict(list)
        for region in panel["regions"]:
            regions[region["name"]].append(region)
        for region_name, region_list in regions.items():
            if len(region_list) > 1:
                duplicates[panel_name]["regions"][region_name] = region_list

    if not duplicates:
        print("\nNo duplicate genes or regions found for any panels.")

    return duplicates


def try_and_remove_duplicates_for_gene(panel_name, gene_name, gene_dup_list):
    """
    Look through duplicate entries of a gene in a panel. If the only difference
    between the duplicates is mode of inheritance (likely because the panel is
    a superpanel and the gene has multiple modes of inheritance per individual
    panel) then just keep one version (dict) for that gene, with the mode
    of inheritance set to 'Other'

    Parameters
    ----------
    panel_name : str
        the panel name
    gene_name : str
        the gene name
    gene_dup_list : list
        list of dicts, each being a duplicate entry for that gene in one panel

    Returns
    -------
    list_of_entries_for_gene : list
        list containing one (or multiple dicts) for that gene in the panel
    """
    entries_for_gene = []
    set_dict = defaultdict(set)

    # For that gene, loop over the duplicate gene dictionaries we have
    # and add the respective key values to the set dictionary
    print(f"Duplicates for gene '{gene_name}' in panel {panel_name} are:")
    for dup in gene_dup_list:
        print(json.dumps(dup, indent=4))
        set_dict["transcript"].add(dup["transcript"])
        set_dict["hgnc_id"].add(dup["hgnc_id"])
        set_dict["confidence_level"].add(dup["confidence_level"])
        set_dict["mode_of_inheritance"].add(dup["mode_of_inheritance"])
        set_dict["mode_of_pathogenicity"].add(dup["mode_of_pathogenicity"])
        set_dict["penetrance"].add(dup["penetrance"])
        set_dict["gene_justification"].add(dup["gene_justification"])
        set_dict["transcript_justification"].add(
            dup["transcript_justification"]
        )
        set_dict["alias_symbols"].add(dup["alias_symbols"])

    # Get any keys for gene where we have multiple diff values for the key
    keys_with_diffs = []
    for element, unique_values in set_dict.items():
        if len(unique_values) > 1:
            keys_with_diffs.append(element)

    # If the only difference is in the key mode of inheritance that's fine,
    # save just the first instance of that gene but update the mode of
    # inheritance to 'Other', keeping all other attributes the same
    if keys_with_diffs and all(
        elem == "mode_of_inheritance" for elem in keys_with_diffs
    ):
        print(
            "Only the mode of inheritance is different between the "
            f"duplicates for {gene_name}, changing the mode of inheritance"
            " to 'Other'"
        )
        single_dict_for_gene = gene_dup_list[0]
        single_dict_for_gene["mode_of_inheritance"] = "Other"

        entries_for_gene.append(single_dict_for_gene)
    else:
        # Not just MOI is different - keep all and print warning with the
        # keys that are different
        number_of_dups = len(gene_dup_list)
        print(
            "There are unexpected differences in keys"
            f" {keys_with_diffs} between the duplicates for gene"
            f" {gene_name} in {panel_name}All of the {number_of_dups} entries"
            " for this gene have been added Please check manually to decide"
            " what to do with any duplicates"
        )
        entries_for_gene.extend(gene_dup_list)

    return entries_for_gene


def try_and_remove_duplicates_for_region(
    panel_name, region_name, region_dup_list
):
    """
    Look through duplicate entries of a region in a panel. If the only
    difference between the duplicates is mode of inheritance (likely because
    the panel is a superpanel and the region has multiple modes of inheritance
    per individual panel) then just keep one version (dict) for that region,
    with the mode of inheritance set to 'Other'

    Parameters
    ----------
    panel_name : str
        name of the panel
    region_name : str
        name of the region
    region_dup_list : list
        list of dicts, each a duplicate entry for that region in one panel

    Returns
    -------
    list_of_entries_for_region : list
        list containing one (or multiple dicts) for that region in the panel
    """
    list_of_entries_for_region = []
    set_dict = defaultdict(set)

    print(
        f"Duplicates found for region '{region_name}' in panel '{panel_name}'."
        " These are:"
    )
    # Loop over the duplicates found for that region, and create a dictionary
    # containing each key and the unique values found for that key in the list
    # of duplicates
    for dup in region_dup_list:
        print(json.dumps(dup, indent=4))
        set_dict["confidence_level"].add(dup["confidence_level"])
        set_dict["mode_of_inheritance"].add(dup["mode_of_inheritance"])
        set_dict["mode_of_pathogenicity"].add(dup["mode_of_pathogenicity"])
        set_dict["penetrance"].add(dup["penetrance"])
        set_dict["chrom"].add(dup["chrom"])
        set_dict["start_37"].add(dup["start_37"])
        set_dict["end_37"].add(dup["end_37"])
        set_dict["start_38"].add(dup["start_38"])
        set_dict["end_38"].add(dup["end_38"])
        set_dict["type"].add(dup["type"])
        set_dict["variant_type"].add(dup["variant_type"])
        set_dict["required_overlap"].add(dup["required_overlap"])
        set_dict["haploinsufficiency"].add(dup["haploinsufficiency"])
        set_dict["triplosensitivity"].add(dup["triplosensitivity"])
        set_dict["justification"].add(dup["justification"])

    # Find any keys with more than one unique value
    keys_with_diffs = []
    for element, unique_vals in set_dict.items():
        if len(unique_vals) > 1:
            keys_with_diffs.append(element)

    # If the only difference is mode of inheritance then that's fine,
    # keep only the first dictionary entry but update the mode of inheritance
    # to 'Other' (keep all other attributes the same)
    if keys_with_diffs and all(
        elem == "mode_of_inheritance" for elem in keys_with_diffs
    ):
        print(
            "Only the mode of inheritance is different between the "
            f"duplicates for '{region_name}' in panel '{panel_name}'. "
            "Keeping only the first entry but changing the mode of inheritance"
            " to 'Other'"
        )

        single_dict_for_region = region_dup_list[0]
        single_dict_for_region["mode_of_inheritance"] = "Other"
        list_of_entries_for_region.append(single_dict_for_region)
    else:
        # Not just MOI is different - keep all and print warning with the
        # keys that are different
        number_of_dups = len(region_dup_list)
        print(
            f"There are unexpected differences in keys {keys_with_diffs} "
            f"between the duplicates for region '{region_name}' in panel "
            f"'{panel_name}'. All of the {number_of_dups} entries for this "
            "region have been added. Please check manually to decide what to "
            "do with any duplicates"
        )
        list_of_entries_for_region.extend(region_dup_list)

    return list_of_entries_for_region


def get_final_list_of_panels(all_panels, duplicates):
    """
    Loop over each panel and try and remove any duplicates in genes or regions
    we've found. Add the hopefully unduplicated set of genes and regions
    for that panel (as a dict) to a final list

    Parameters
    ----------
    all_panels : list
        list of dicts, each representing a panel
    panel_entity_counts : dict
        dict containing counts of each gene and region within each panel

    Returns
    -------
    final_list_of_panels : list
        list of dicts, each dict representing a panel (with dup genes or
        regions removed)
    """
    final_list_of_panels = []

    for panel in all_panels:
        panel_name = panel["panel_name"]

        updated_genes = []
        processed_genes = set()
        for gene in panel["genes"]:
            gene_symbol = gene["gene_symbol"]
            # If we have dups for that gene (and we haven't already processed
            # it), try and get just one version and add to updated_genes
            if gene_symbol in duplicates.get(panel_name, {}).get("genes", {}):
                if gene_symbol not in processed_genes:
                    gene_dups = duplicates[panel_name]["genes"][gene_symbol]
                    gene_without_dups = try_and_remove_duplicates_for_gene(
                        panel_name, gene_symbol, gene_dups
                    )
                    updated_genes.extend(gene_without_dups)
                    processed_genes.add(gene_symbol)
            # If we don't have dups for that gene, just add the info to
            # updated_genes
            else:
                updated_genes.append(gene)
                processed_genes.add(gene_symbol)

        # Do same for regions
        updated_regions = []
        processed_regions = set()
        for region in panel["regions"]:
            region_name = region["name"]
            if region_name in duplicates.get(panel_name, {}).get(
                "regions", {}
            ):
                if region_name not in processed_regions:
                    region_dups = duplicates[panel_name]["regions"][
                        region_name
                    ]
                    region_without_dups = try_and_remove_duplicates_for_region(
                        panel_name, region_name, region_dups
                    )
                    updated_regions.extend(region_without_dups)
                    processed_regions.add(region_name)
            else:
                updated_regions.append(region)
                processed_regions.add(region_name)

        # Add the final info for that panel to our list
        final_list_of_panels.append(
            {
                "panel_source": panel["panel_source"],
                "panel_name": panel_name,
                "external_id": panel["external_id"],
                "panel_version": panel["panel_version"],
                "genes": updated_genes,
                "regions": updated_regions,
            }
        )

    assert len(all_panels) == len(final_list_of_panels), (
        f"The number of panels has changed from {len(all_panels)} originally"
        f"to {len(final_list_of_panels)} when removing duplicates"
    )

    return final_list_of_panels


def main():
    args = parse_args()

    panel_ids_to_keep = None
    if args.genepanels:
        genepanels_df = read_in_genepanels(args.genepanels)
        panel_ids_to_keep = get_unique_required_panel_ids(genepanels_df)

    if args.extra_panels:
        if not args.genepanels:
            print(
                "Please provide a genepanels file with --genepanels if you"
                " want to include extra panels with --extra_panels. Exiting"
            )
            sys.exit(1)
        else:
            panel_ids_to_keep = add_additional_panel_ids(
                panel_ids_to_keep, args.extra_panels
            )

    # Get all signed off panels as a list of dicts, one per panel
    all_required_panels = parse_specified_pa_panels(panel_ids_to_keep)

    # Find any duplicate genes or regions in the panels
    # If duplicates, try and keep only one if it's just MOI that's different
    duplicates = find_duplicate_genes_or_regions(all_required_panels)
    final_panels = get_final_list_of_panels(all_required_panels, duplicates)
    # Save updated version to JSON file
    with open(args.outfile_name, "w", encoding="utf8") as panelapp_dump:
        json.dump(final_panels, panelapp_dump, indent=4)


if __name__ == "__main__":
    main()
