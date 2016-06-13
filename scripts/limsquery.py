
from genologics.entities import *
from genologics.lims import *
from genologics.config import BASEURI, USERNAME, PASSWORD




lims= Lims(BASEURI, USERNAME, PASSWORD)


def  get_udfs():

    step_name='End repair, A-tailing and adapter ligation (TruSeq RNA) 4.0'
    udf_name="Lot no: Adaptor Plate"

    pros=lims.get_processes(type=step_name)
    for pro in pros:
        try:
            print "{}\t{} {}\t{}".format(pro.date_run,pro.technician.first_name,pro.technician.last_name ,pro.udf[udf_name] )
        except:
            print "{}\t{} {}".format(pro.date_run,pro.technician.first_name,pro.technician.last_name)

get_udfs()
