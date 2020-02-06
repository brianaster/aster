from sqlalchemy import or_, not_, and_, func

from core.helpers import *
from core.models import *


START_AGE = 21
END_AGE = 75
MEASURE_START_YEAR = 2017
MEASURE_END_YEAR = 2017

MEMBERS_START_DATE = "19410101"
MEMBERS_END_DATE = "19951231"

measure = Measure(__file__.split('\\')[-1].split('.')[0])
conn, session = get_database_connection_and_session()


estrogen_medications = get_medications_codes_list(["Estrogen Agonists Medications"])

uu = {}

def get_eligible_members(members, visits):
    exclude_members_ids = []
    eligible_members = set()
    for i in visits:
        if not ((i.member_age in range(21, 76) and i.member.gender == 'M') or (i.member_age in range(40, 76) and i.member.gender == 'F')):
            continue
        print(i)
        if i.memberId in exclude_members_ids:
            continue
        if not uu.get(i.memberId):
            uu.update({
                i.memberId: {
                    "Acute Inpatient": set(),
                    "Outpatient": set(),
                    "Observation": set(),
                    "ED": set(),
                    "Nonacute Inpatient": set(),
                    "Telephone": set(),
                    "Online Assessments": set(),
                    "Inpatient MI": set(),
                    "CABG": set(),
                    "PCI": set(),
                    "Other Revascularization": set(),
                    "exclude": {
                        "Acute Inpatient": set(),
                        "Outpatient": set(),
                        "Observation": set(),
                        "ED": set(),
                        "Nonacute Inpatient": set(),
                        "Pregnancy": set(),
                        "IVF": set(),
                        "ESRD": set(),
                        "Cirrhosis": set(),
                        "Muscular Pain and Disease": set(),
                    }
                    "IPSD": {},
                }
            })

        if check_daterange(i, 2016, 2016):
            inpatient_mi = measure.check_conditions(
                i,
                ["Inpatient Stay", "MI"],
            )
            if inpatient_mi:
                uu[i.memberId]["Inpatient MI"].add(inpatient_mi)

            cabg = measure.check_conditions(
                i,
                ["CABG"],
            )
            if cabg:
                uu[i.memberId]["CABG"].add(cabg)

            pci = measure.check_conditions(
                i,
                ["PCI"],
            )
            if pci:
                uu[i.memberId]["PCI"].add(pci)


            other_revascularization = measure.check_conditions(
                i,
                ["Other Revascularization"],
            )
            if other_revascularization:
                uu[i.memberId]["Other Revascularization"].add(other_revascularization)

        if check_daterange(i, 2016, 2017):
            outpatient_ivd = measure.check_conditions(
                i,
                ["Outpatient", "IVD"],
            )
            if outpatient_ivd:
                uu[i.memberId]["Outpatient"].add(i)

            telephone_ivd = measure.check_conditions(
                i,
                ["Telephone Visits", "IVD"],
            )
            if telephone_ivd:
                uu[i.memberId]["Telephone"].add(telephone_ivd)


            online_assessments = measure.check_conditions(
                i,
                ["Online Assessments ", "IVD"],
            )
            if online_assessments:
                uu[i.memberId]["Online Assessments"].add(online_assessments)


            acute_inpatient = measure.check_conditions(
                i,
                ["Acute Inpatient", "IVD"],
            )
            if acute_inpatient:
                uu[i.memberId]["Acute Inpatient"].add(acute_inpatient)


        if check_daterange(i, 2016, 2017):
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

            acute_inpatient = measure.check_conditions(
                i,
                ["Acute Inpatient", "Advanced Illness "],
            )
            if acute_inpatient:
                uu[i.memberId]["exclude"]["Acute Inpatient"].add(acute_inpatient)


        if check_daterange(i, 2016, 2017):
            pregnancy = measure.check_conditions(
                i,
                ["Pregnancy",],
            )
            if pregnancy:
                uu[i.memberId]["exclude"]["Pregnancy"].add(pregnancy)

            ivf = measure.check_conditions(
                i,
                ["IVF",],
            )
            if ivf:
                uu[i.memberId]["exclude"]["IVF"].add(ivf)


            esrd = measure.check_conditions(
                i,
                ["ESRD",],
            )
            if esrd:
                uu[i.memberId]["exclude"]["ESRD"].add(esrd)


            cirrhosis = measure.check_conditions(
                i,
                ["Cirrhosis"],
            )
            if cirrhosis:
                uu[i.memberId]["exclude"]["Cirrhosis"].add(cirrhosis)

        if check_daterange(i, 2017, 2017):
            muscular = measure.check_conditions(
                i,
                ["Muscular Pain and Disease"],
            )
            if muscular:
                uu[i.memberId]["exclude"]["Muscular Pain and Disease"].add(muscular)


    for i in uu:
        encounters = len(uu[i]["Outpatient"] | uu[i]["Telephone"] | uu[i]["Online Assessments"] | uu[i]["Acute Inpatient"])
        if encounters >= 2:
            eligible_members.add(i)
        else:
            telehealth_count = 0
            for j in uu[i]["Acute Inpatient"]:
                if measure.check_conditions(
                    j,
                    ["Telehealth Modifier "]
                ) or measure.check_conditions(
                    j,
                    ["Telehealth POS", ""]
                ):
                    telehealth_count += 1
            if (len(uu[i]["Acute Inpatient"]) > telehealth_count and len(uu[i]["Acute Inpatient"])) >= 2 or \
                (len(uu[i]["Acute Inpatient"]) == 1 and telehealth_count <= 1 and encounters == 1):
                    eligible_members.add(i)
        if uu[i]["Inpatient MI"] or uu[i]["CABG"] or uu[i]["PCI"] or uu[i]["Inpatient MI"] or uu[i]["Other Revascularization"]:
            eligible_members.add(i)


    pharm = session.query(Pharmacy).filter(
        Pharmacy.ndc_DrugCode.in_(estrogen_medications),
        Pharmacy.serviceDate.between("20160101", "20171231"),
    )
    for i in pharm:
        eligible_members.add(i.memberId)

    for i in uu:
        if len(uu[i]["Acute Inpatient"]) >= 1:
            eligible_members.add(i)
        if len(uu[i]["Outpatient"] | uu[i]["Observation"] | uu[i]["ED"] | uu[i]["Nonacute Inpatient"]) >= 2:
            eligible_members.add(i)

    for i in uu:
        if i in eligible_members:
            if len(uu[i]["exclude"]["Outpatient"] | uu[i]["exclude"]["Observation"] | uu[i]["exclude"]["ED"] | uu[i]["exclude"]["Acute Inpatient"]) >= 2 and i.member_age >= 66:
                eligible_members.remove(i)
                continue
            if len(uu[i]["exclude"]["Pregnancy"] | uu[i]["exclude"]["IVF"] | uu[i]["exclude"]["ESRD"] | uu[i]["exclude"]["Cirrhosis"]) >= 1 or (uu[i]["exclude"]["Acute Inpatient"] >= 1 and i.member_age >= 66):
                eligible_members.remove(i)
                continue

    eligible_members = exclude_dispenced_dementia(eligible_members)

    return eligible_members





members, visits = get_members_and_visits(MEMBERS_START_DATE, MEMBERS_END_DATE)

eligible_members = get_eligible_members(members, visits)

print(eligible_members)
print(len(eligible_members))


statin_medications = get_medications_codes_list(["High and Moderate-Intensity Statin Medications"])

pharm = session.query(Pharmacy).filter(
    Pharmacy.ndc_DrugCode.in_(statin_medications),
    Pharmacy.serviceDate.between("20170101", "20171231"),
).order_by(Pharmacy.serviceDate)

numerator = set()

for i in pharm:
    numerator.add(i.memberId)
    if not uu[i.memberId].get('IPSD'):
        uu[i.memberId]["IPSD"] = {
            "Service Date": i.serviceDate,
            "Days Covered": min(get_dates_diff(i.serviceDate, "20171231"), i.daysSupply),
            "End Supply Period": min(add_days_to_date(i.serviceDate, i.daysSupply), "20171231"),
            "Treatment Period": get_dates_diff(i.serviceDate, "20171231"),
        }
    else:
        uu[i.memberId]["Days Covered"] += max(
            0, get_dates_diff(
                uu[i.memberId]["IPSD"]["End Supply Period"],
                min(add_days_to_date(i.serviceDate, i.daysSupply), "20171231"),
            )
        )
        uu[i.memberId]["End Supply Period"] = max(
            min(add_days_to_date(i.serviceDate, i.daysSupply), "20171231"), 
            uu[i.memberId]["IPSD"]["End Supply Period"],
        )

print(len(numerator), len(eligible_members))

numerator2 = set()
for i in numerator:
    if uu[i]["IPSD"]["Days Covered"] / uu[i]["IPSD"]["Treatment Period"] >= 0.8:
        numerator2.add(i)
