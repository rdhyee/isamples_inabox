import pytest
from isamples_metadata.GEOMETransformer import GEOMETransformer

iterator_testcases = [
    ("1187/SU/KS/2006, 04239/ SU.3/KS/2019",
     {"authorizedBy": ["1187/SU/KS/2006", "04239/SU.3/KS/2019"], "compliesWith": []}),
    ("LIPI 1187/SU/KS/2006 and 04239/SU.3/KS/2006",
     {"authorizedBy": ["LIPI 1187/SU/KS/2006", "04239/SU.3/KS/2006"], "compliesWith": []}),
    (
        "Commonwealth of Australia Torres Strait Fisheries Act 1984 Permit for Scientific purposes; Australian "
        "Fisheries "
        "Management Authority, Thursday Island",
        {"authorizedBy": ["Commonwealth of Australia Torres Strait Fisheries Act 1984 Permit for Scientific purposes",
                          "Australian Fisheries Management Authority, Thursday Island"], "compliesWith": []}),
    ("nan", {"authorizedBy": [], "compliesWith": []}),
    ("NA", {"authorizedBy": [], "compliesWith": []}),
    ("na", {"authorizedBy": [], "compliesWith": []}),
    ("unknown", {"authorizedBy": [], "compliesWith": []}),
    ("None_required", {"authorizedBy": [], "compliesWith": []}),
    ('"G08/28114.1; Queensland Government"',
     {"authorizedBy": ["G08/28114.1", "Queensland Government"], "compliesWith": []}),
    ("AllPermitInfoAvailable in working files Coralie TAQUET-folderInNadaokaLab",
     {"authorizedBy": ["AllPermitInfoAvailable in working files Coralie TAQUET-folderInNadaokaLab"],
      "compliesWith": []}),
    ('"Collected under gratuitous permit from BFAR to ODU and UPMSI, amended October 2, 2008"',
     {"authorizedBy": ["Collected under gratuitous permit from BFAR to ODU and UPMSI, amended October 2, 2008"],
      "compliesWith": []}),
    ("research visa no. 10350008304; Dept Env & Conservation/Nat Research Institute",
     {"authorizedBy": ["research visa no. 10350008304", "Dept Env & Conservation/Nat Research Institute"],
      "compliesWith": []}),
    (
        '"Ministufffdrio da Aquicultura e Pescas, Direcufffdufffdo Nacional de Pescas e Aquicultura (authorised by A '
        'Fernandes, L Fontes, J Freitas; guia de marssa: 502/DNPA/VIII/10 and 452/DNPA/VII/11). Export of samples was '
        'authorised by the Departmento de Quarentena das Pescas (export permit: 162/FQ006/EXP./DNQB/VII/2011)"',
        {"authorizedBy": [
            "Ministufffdrio da Aquicultura e Pescas, Direcufffdufffdo Nacional de Pescas e Aquicultura (authorised by "
            "A Fernandes, L Fontes, J Freitas; guia de marssa: 502/DNPA/VIII/10 and 452/DNPA/VII/11). Export of "
            "samples was authorised by the Departmento de Quarentena das Pescas (export permit: "
            "162/FQ006/EXP./DNQB/VII/2011)"],
            "compliesWith": []}),
    ("NMFS permits, state permits, & CITES permits",
     {"authorizedBy": ["NMFS permits, state permits, & CITES permits"], "compliesWith": []}),
    ("MinistryofWaterandEnvironmentofYemen",
     {"authorizedBy": ["MinistryofWaterandEnvironmentofYemen"], "compliesWith": []}),
    (
        "The SPICE project (Science for the protection of indonesian Coastal marine Ecosystems) was conducted and permitted under the governmental agreement between the German Federal Ministry of Education and Research (BMBF) and the Indonesian Ministry for Research and Technology (RISTEK), Indonesian Institute of Sciences (LIPI), Indonesian Ministry of Maritime Affairs and Fisheries (DKP), and Agency for the Assessment and Application of Technology (BPPT). This work was carried out in co-operation with Hassanuddin University (UNHAS, Makassar, Indonesia), Agricultural University Bogor (IPB, Bogor, Indonesia), and Jenderal Soedirman University (Purwokerto, Indonesia).",
        {"authorizedBy": [
            "The SPICE project (Science for the protection of indonesian Coastal marine Ecosystems) was conducted and permitted under the governmental agreement between the German Federal Ministry of Education and Research (BMBF) and the Indonesian Ministry for Research and Technology (RISTEK), Indonesian Institute of Sciences (LIPI), Indonesian Ministry of Maritime Affairs and Fisheries (DKP), and Agency for the Assessment and Application of Technology (BPPT). This work was carried out in co-operation with Hassanuddin University (UNHAS, Makassar, Indonesia), Agricultural University Bogor (IPB, Bogor, Indonesia), and Jenderal Soedirman University (Purwokerto, Indonesia)."],
            "compliesWith": []}),
    ("DAFF/DEA", {"authorizedBy": ["DAFF", "DEA"], "compliesWith": []}),
    ("Ministry of Environment, Natural Resources, and Transport",
     {"authorizedBy": ["Ministry of Environment, Natural Resources, and Transport"], "compliesWith": []}),
    ("G10/33597.1 & 118636; GBRMPA & QLD Fisheries",
     {"authorizedBy": ["G10/33597.1 & 118636", "GBRMPA & QLD Fisheries"], "compliesWith": []})
]


@pytest.mark.parametrize("original_entries,expected_outcome", iterator_testcases)
def test_split_string(original_entries, expected_outcome):
    assert GEOMETransformer.parse_permit_freetext(original_entries) == expected_outcome


structured_data_test_values = [
    (
        "complies_with:foo authorized_by:bar",
        ["foo"],
        ["bar"],
    ),
    (
        "Complies_with:foo Authorized_by:bar",
        ["foo"],
        ["bar"],
    ),
    (
        "complies_with:foo",
        ["foo"],
        None,
    ),
    (
        "authorized_by:bar",
        None,
        ["bar"],
    ),
    (
        "complies_with:foo1,foo2 authorized_by:bar1,bar2",
        ["foo1", "foo2"],
        ["bar1", "bar2"],
    ),
    (
        "complies_with:foo1;foo2 authorized_by:bar1;bar2",
        ["foo1", "foo2"],
        ["bar1", "bar2"],
    ),
    (
        "compliesWith:foo1;foo2 authorizedBy:bar1;bar2",
        ["foo1", "foo2"],
        ["bar1", "bar2"],
    ),
    (
        "complies with:foo1;foo2 authorized by:bar1;bar2",
        ["foo1", "foo2"],
        ["bar1", "bar2"],
    ),
    (
        "complies:foo1;foo2 authorized:bar1;bar2",
        ["foo1", "foo2"],
        ["bar1", "bar2"],
    ),
    (
        "authorized:foo complies:bar",
        ["bar"],
        ["foo"],
    ),
]


@pytest.mark.parametrize("source_text,complies_with,authorized_by", structured_data_test_values)
def test_parse_structured_data(source_text: str, complies_with: list[str], authorized_by: list[str]):
    parse_result = GEOMETransformer.parse_permit_structured_text(source_text)
    assert complies_with == parse_result.get("compliesWith")
    assert authorized_by == parse_result.get("authorizedBy")
