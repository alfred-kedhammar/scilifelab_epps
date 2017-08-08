#!/usr/bin/env python
from argparse import ArgumentParser
from genologics.lims import Lims
from genologics.config import BASEURI,USERNAME,PASSWORD
from genologics.entities import Process


def main(args):
    log = []
    lims = Lims(BASEURI,USERNAME,PASSWORD)
    process = Process(lims, id=args.pid)
    for swp_iomap in process.input_output_maps:
        if swp_iomap[1]['output-generation-type'] !=  'PerInput':
            continue
        inp_artifact = swp_iomap[0]['uri']
        amount_check_pros = lims.get_processes(type='Amount confirmation QC', inputartifactlimsid=inp_artifact.id)
        amount_check_pros.sort(reverse=True, key=lambda x:x.date_run)
        try:
            correct_amount_check_pro = amount_check_pros[0]
        except KeyError:
            sys.exit("Cannot find an Amount Confirmation QC step for artifact {}".format(inp_artifact.id))
        else:
            for iomap in correct_amount_check_pro.input_output_maps:
                if iomap[1]['output-generation-type'] !=  'PerInput':
                    continue
                if iomap[0]['limsid'] == inp_artifact.id:
                    for udf_name in ['Concentration', 'Conc. Units', 'Total Volume (uL)']:
                        try:
                            swp_iomap[1]['uri'].udf[udf_name] = iomap[1]['uri'].udf[udf_name]
                        except:
                            import pdb;pdb.set_trace()
                    swp_iomap[1]['uri'].udf['Amount taken (ng)'] = iomap[1]['uri'].udf['Amount to take (ng)']
                    swp_iomap[1]['uri'].put()





if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument('--pid',
                        help='Lims id for current Process', required=True)
    args = parser.parse_args()
    main(args)
