import json
import zipfile
from typing import BinaryIO

import polars as pl

schema = pl.Schema(
    {
        'results': pl.List(
            pl.Struct(
                {
                    'authoritynumb': pl.String(),
                    'companynumb': pl.String(),
                    'duplicate': pl.String(),
                    'fulfillexpeditecriteria': pl.String(),
                    'occurcountry': pl.String(),
                    'patient': pl.Struct(
                        {
                            'drug': pl.List(
                                pl.Struct(
                                    {
                                        'actiondrug': pl.String(),
                                        'activesubstance': pl.Struct({'activesubstancename': pl.String()}),
                                        'drugadditional': pl.String(),
                                        'drugadministrationroute': pl.String(),
                                        'drugauthorizationnumb': pl.String(),
                                        'drugbatchnumb': pl.String(),
                                        'drugcharacterization': pl.String(),
                                        'drugcumulativedosagenumb': pl.String(),
                                        'drugcumulativedosageunit': pl.String(),
                                        'drugdosageform': pl.String(),
                                        'drugdosagetext': pl.String(),
                                        'drugenddate': pl.String(),
                                        'drugenddateformat': pl.String(),
                                        'drugindication': pl.String(),
                                        'drugintervaldosagedefinition': pl.String(),
                                        'drugintervaldosageunitnumb': pl.String(),
                                        'drugrecurreadministration': pl.String(),
                                        'drugseparatedosagenumb': pl.String(),
                                        'drugstartdate': pl.String(),
                                        'drugstartdateformat': pl.String(),
                                        'drugstructuredosagenumb': pl.String(),
                                        'drugstructuredosageunit': pl.String(),
                                        'drugtreatmentduration': pl.String(),
                                        'drugtreatmentdurationunit': pl.String(),
                                        'medicinalproduct': pl.String(),
                                        'openfda': pl.Struct(
                                            {
                                                'application_number': pl.List(pl.String()),
                                                'brand_name': pl.List(pl.String()),
                                                'generic_name': pl.List(pl.String()),
                                                'manufacturer_name': pl.List(pl.String()),
                                                'nui': pl.List(pl.String()),
                                                'package_ndc': pl.List(pl.String()),
                                                'pharm_class_cs': pl.List(pl.String()),
                                                'pharm_class_epc': pl.List(pl.String()),
                                                'pharm_class_moa': pl.List(pl.String()),
                                                'pharm_class_pe': pl.List(pl.String()),
                                                'product_ndc': pl.List(pl.String()),
                                                'product_type': pl.List(pl.String()),
                                                'route': pl.List(pl.String()),
                                                'rxcui': pl.List(pl.String()),
                                                'spl_id': pl.List(pl.String()),
                                                'spl_set_id': pl.List(pl.String()),
                                                'substance_name': pl.List(pl.String()),
                                                'unii': pl.List(pl.String()),
                                            }
                                        ),
                                    }
                                )
                            ),
                            'patientagegroup': pl.String(),
                            'patientonsetage': pl.String(),
                            'patientonsetageunit': pl.String(),
                            'patientsex': pl.String(),
                            'patientweight': pl.String(),
                            'reaction': pl.List(
                                pl.Struct(
                                    {
                                        'reactionmeddraversionpt': pl.String(),
                                        'reactionmeddrapt': pl.String(),
                                        'reactionoutcome': pl.String(),
                                    }
                                )
                            ),
                            'summary': pl.Struct({'narrativeincludeclinical': pl.String()}),
                        }
                    ),
                    'primarysource': pl.Struct(
                        {
                            'literaturereference': pl.String(),
                            'qualification': pl.String(),
                            'reportercountry': pl.String(),
                        }
                    ),
                    'primarysourcecountry': pl.String(),
                    'receiptdate': pl.String(),
                    'receiptdateformat': pl.String(),
                    'receivedate': pl.String(),
                    'receivedateformat': pl.String(),
                    'receiver': pl.Struct({'receivertype': pl.String(), 'receiverorganization': pl.String()}),
                    'reportduplicate': pl.Struct({'duplicatesource': pl.String(), 'duplicatenumb': pl.String}),
                    'reporttype': pl.String(),
                    'safetyreportid': pl.String(),
                    'safetyreportversion': pl.String(),
                    'sender': pl.Struct({'sendertype': pl.String(), 'senderorganization': pl.String()}),
                    'serious': pl.String(),
                    'seriousnesscongenitalanomali': pl.String(),
                    'seriousnessdeath': pl.String(),
                    'seriousnessdisabling': pl.String(),
                    'seriousnesshospitalization': pl.String(),
                    'seriousnesslifethreatening': pl.String(),
                    'seriousnessother': pl.String(),
                    'transmissiondate': pl.String(),
                    'transmissiondateformat': pl.String(),
                }
            )
        ),
    }
)


def transform(src: BinaryIO, dst: BinaryIO) -> None:
    with zipfile.ZipFile(src, 'r') as zip_file:
        for item in zip_file.filelist:
            with zip_file.open(item) as file:
                json_file = json.load(file)
                initial = pl.DataFrame(json_file['results'], schema=schema, strict=False)
                results = initial.select('results').explode('results').unnest('results')
                results.write_parquet(dst)
