from sqlalchemy import or_, not_, and_, func

from core.helpers import *
from core.models import *

STARS_RANGES = {
    (0, 0.60): 1,
    (0.60, 0.74): 2,
    (0.74, 0.79): 3,
    (0.79, 0.84): 4,
    (0.84, 1): 5,
}
START_AGE = 18
END_AGE = 200
MEASURE_START_YEAR = 2016
MEASURE_END_YEAR = 2016
MEMBERS_START_DATE = "19000101"
MEMBERS_END_DATE = "19981231"

measure = Measure(__file__.split('\\')[-1].split('.')[0])
conn, session = get_database_connection_and_session()
medications = get_medications_codes_list(["DMARD Medications"])


uu = {}

def get_eligible_members(members, visits):
    exclude_members_ids = []
    eligible_members = set()
    for i in visits:
        if i.memberId in exclude_members_ids:
            continue
        if not uu.get(i.memberId):
            uu.update({
                i.memberId: {
                    "Outpatient": set(),
                    "Telephone": set(),
                    "Nonacute Inpatient": set(),
                    "Nonacute Inpatient Telehealth": set(),
                    "Online Assessments": set(),
                    "Inpatient Stay": set(),
                    "Nonacute Inpatient Stay": set(),
                    "exclude": {
                        "Acute Inpatient": set(),
                        "Outpatient": set(),
                        "Observation": set(),
                        "ED": set(),
                        "Nonacute Inpatient": set(),
                    }
                }
            })

        if check_daterange(i, 2016, 2016, end_day="1130"):
            outpatient = measure.check_conditions(
                i,
                ["Outpatient", "Rheumatoid Arthritis"],
            )
            if outpatient:
                uu[i.memberId]["Outpatient"].add(outpatient)


            telephone = measure.check_conditions(
                i,
                ["Telephone Visits", "Rheumatoid Arthritis"],
            )
            if telephone:
                uu[i.memberId]["Observation"].add(telephone)

            online_assessment = measure.check_conditions(
                i,
                ["Online Assessments ", "Rheumatoid Arthritis"]
            )
            if online_assessment:
                uu[i.memberId]["Online Assessments"].add(online_assessment)

            inpatient_stay = measure.check_conditions(
                i,
                ["Inpatient Stay",],
            )
            if inpatient_stay:
                uu[i.memberId]["Inpatient Stay"].add(i)

            nonacute_inpatient_stay = measure.check_conditions(
                i,
                ["Nonacute Inpatient Stay", "Rheumatoid Arthritis"],
            )
            if nonacute_inpatient_stay:
                uu[i.memberId]["Nonacute Inpatient Stay"].add(i)

            acute_inpatient = measure.check_conditions(
                i,
                ["Acute Inpatient", "Advanced Illness "],
            )
            if acute_inpatient:
                uu[i.memberId]["exclude"]["Acute Inpatient"].add(acute_inpatient)


            outpatient = measure.check_conditions(
                i,
                ["Outpatient", "Advanced Illness "],
            )
            if outpatient:
                uu[i.memberId]["exclude"]["Outpatient"].add(outpatient)


            observation = measure.check_conditions(
                i,
                ["Observation", "Advanced Illness "],
            )
            if observation:
                uu[i.memberId]["exclude"]["Observation"].add(observation)


            ed = measure.check_conditions(
                i,
                ["ED", "Advanced Illness "],
            )
            if ed:
                uu[i.memberId]["exclude"]["ED"].add(ed)


            nonacute_inpatient = measure.check_conditions(
                i,
                ["Nonacute Inpatient", "Advanced Illness "],
            )
            if nonacute_inpatient:
                uu[i.memberId]["exclude"]["Nonacute Inpatient"].add(nonacute_inpatient)


    for i in uu:
        encounters = len(uu[i]["Outpatient"] | uu[i]["Telephone"] | uu[i]["Online Assessments"] | uu[i]["Nonacute Inpatient Stay"])
        if encounters >= 2:
            eligible_members.add(i)
        else:
            telehealth_count = 0
            for j in uu[i]["Nonacute Inpatient Stay"]:
                if measure.check_conditions(
                    j,
                    ["Telehealth Modifier "]
                ) or measure.check_conditions(
                    j,
                    ["Telehealth POS", "Outpatient"]
                ):
                    telehealth_count += 1
            if (len(uu[i]["Nonacute Inpatient Stay"]) > telehealth_count and len(uu[i]["Nonacute Inpatient Stay"])) >= 2 or \
                (len(uu[i]["Nonacute Inpatient Stay"]) == 1 and telehealth_count <= 1 and encounters == 1):
                    eligible_members.add(i)


    for i in uu:
        if i in eligible_members:
            if len(uu[i]["exclude"]["Outpatient"] | uu[i]["exclude"]["Observation"] | uu[i]["exclude"]["ED"] | uu[i]["exclude"]["Nonacute Inpatient"]) >= 2:
                eligible_members.remove(i)
                continue
            if len(uu[i]["exclude"]["Acute Inpatient"]) >= 1:
                eligible_members.remove(i)
                continue

    eligible_members = exclude_dispenced_dementia(eligible_members, ages=(0, 200))

    pharm = session.query(Pharmacy).filter(
        Pharmacy.ndc_DrugCode.in_(medications),
        Pharmacy.serviceDate.between("20150101", "20161231"),
    )
    for i in pharm:
        eligible_members.add(i.memberId)

    return eligible_members


members, visits = get_members_and_visits(MEMBERS_START_DATE, MEMBERS_END_DATE)

eligible_members = get_eligible_members(members, visits)
print(eligible_members)

for i in visits:
    if i.memberId in eligible_members:
        if measure.check_conditions(i, ["HIV"]) or measure.check_conditions(i, ["Pregnancy"]):
            eligible_members.remove(i.memberId)

numerator = set()

for i in visits:
    if i.memberId in eligible_members:
        if check_daterange(i, 2016, 2016, end_day="1130"):
            if measure.check_conditions(i, ["DMARD"]):
                numerator.add(i.memberId)

print(len(numerator), numerator)
print(len(eligible_members), eligible_members)
print(get_stars_rank(numerator, eligible_members, STARS_RANGES))
