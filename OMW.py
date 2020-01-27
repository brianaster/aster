from sqlalchemy import or_, not_, and_, func

from core.helpers import *
from core.models import *

STARS_RANGES = {
    (0, 0.31): 1,
    (0.31, 0.41): 2,
    (0.41, 0.50): 3,
    (0.50, 0.67): 4,
    (0.67, 1): 5,   
}
START_AGE = 67
END_AGE = 85
MEASURE = __file__.split('\\')[-1].split('.')[0]
MEASURE_START_YEAR = 2017
MEASURE_END_YEAR = 2018

conn, session = get_database_connection_and_session()
codes_dict = get_codes_for_measure(MEASURE)

uu = {}


for i in codes_dict.items():
    print(i)



def get_all_members():
    gm = session.query(GeneralMembership).filter(
        and_(
            GeneralMembership.dateOfBirth >= "{}0701".format(MEASURE_END_YEAR - END_AGE),
            GeneralMembership.dateOfBirth <= "{}0630".format(MEASURE_END_YEAR - START_AGE)
        )
    )
    members = {i.memberId: i for i in gm}

    return exclude_dispenced_dementia(members, ages=(66, 80))


def get_all_visits(members, start=MEASURE_START_YEAR, end=MEASURE_END_YEAR):
    visits = session.query(Visit).filter(
        Visit.memberId.in_(list(members)),
        Visit.dateofService.between(
            "{}0701".format(start),
            "{}0630".format(end),
        ),
    ).order_by(Visit.memberId, Visit.dateofService).all()
    for i in visits:
        age = (int(members[i.memberId].dateOfBirth) - int(i.dateofService)) // 10000
        setattr(i, "member_age", age)
    return visits



def get_eligible_members(members, visits):
    exclude_members_ids = []

    mmbrs = set()

    discharge_dates = {}
    for i in visits:
        if i.memberId in exclude_members_ids:
            continue
        if not uu.get(i.memberId):
            uu.update({
                i.memberId: {
                    "IESD": "99999999",
                    "IESD_inpatient": False,
                    "Admission Date": "99999999"
                }
            })

        for j in codes_dict:
            d = check_for_diagnosis_or_procedures(i, **codes_dict[j])
            if not uu[i.memberId].get(j):
                uu[i.memberId].update({j: 0})
            uu[i.memberId][j] += 1 if 1 in d.values() else 0

        if uu[i.memberId]["Fractures"]:
            if i.admissionDate < uu[i.memberId]["IESD"] and i.admissionDate:
                uu[i.memberId]["IESD"] = i.admissionDate

        if uu[i.memberId]["Fractures"] and (uu[i.memberId]["Acute Inpatient"] or uu[i.memberId]["Nonacute Inpatient"]):
            if not discharge_dates.get(i.memberId):
                discharge_dates.update({i.memberId: "99999999"})
            if i.dischargeDate < discharge_dates[i.memberId] and i.dischargeDate:
                discharge_dates[i.memberId] = i.dischargeDate
                mmbrs.add(i.memberId)


    for i in uu:
        if uu[i]["Fractures"] and ( \
            uu[i]["Outpatient"] or uu[i]["Observation"] or uu[i]["ED"] or \
            (not uu[i]["Telehealth Modifier "] and not uu[i]["Telehealth POS"]) 
        ) and not (uu[i]["ED"] and uu[i]["Inpatient Stay"]):
            mmbrs.add(i)
    

    for i in visits:
        if uu[i.memberId]["Fractures"] and (
            uu[i.memberId]["Outpatient"] or uu[i.memberId]["Telephone Visits"] or uu[i.memberId]["Online Assessments "] or \
            uu[i.memberId]["Observation"] or uu[i.memberId]["ED"] \
        ) and not (uu[i.memberId]['Inpatient Stay'] and uu[i.memberId]["ED"]) and \
            uu[i.memberId]["IESD"] != "99999999" and get_dates_diff(i.dischargeDate, uu[i.memberId]["IESD"]) in range(0, 61):
                exclude_members_ids.append(i.memberId)
        if uu[i.memberId]["Fractures"] and uu[i.memberId]["Inpatient Stay"]:
            if uu[i.memberId]["IESD"] != "99999999" and get_dates_diff(i.dischargeDate, uu[i.memberId]["IESD"]) in range(0, 61):
                exclude_members_ids.append(i.memberId)

    for i in visits:
        if uu[i.memberId]["IESD"] != "99999999" and uu[i.memberId]["Bone Mineral Density Tests"] and str(int(i.dateofService) + 20000) >= uu[i.memberId]["IESD"]:
            exclude_members_ids.append(i.memberId)
        if uu[i.memberId]["IESD"] != "99999999" and uu[i.memberId]["Osteoporosis Medications"] and str(int(i.dateofService) + 10000) >= uu[i.memberId]["IESD"]:
            exclude_members_ids.append(i.memberId)


    for i in members:
        age = (int("{}1231".format(MEASURE_END_YEAR)) - int(members[i].dateOfBirth)) / 10000
        if age >= 81 and uu.get(i):
            if uu[i]["Fractures"]:
                exclude_members_ids.append(i)
        if age >= 66 and age <= 80 and uu.get(i):
            if uu[i]["Advanced Illness "] >= 2 and (uu[i]["Outpatient"] >= 2 or uu[i]["Observation"] >= 2 or uu[i]["ED"] >= 2 or uu[i]["Nonacute Inpatient"] >= 2):
                exclude_members_ids.append(i)
            if uu[i]["Advanced Illness "] and uu[i]["Acute Inpatient"]:
                exclude_members_ids.append(i)





    for i in exclude_members_ids:
        if i in mmbrs:
            mmbrs.remove(i)

    return mmbrs







members = get_all_members()
visits = get_all_visits(members)

members = get_eligible_members(members, visits)

numerator = set()

for i in members:
    print(i, uu[i]["IESD"])

visits = get_all_visits(members, MEASURE_END_YEAR, MEASURE_END_YEAR)

for i in visits:
    if uu[i]["IESD"] != "99999999" and 1 in check_for_diagnosis_or_procedures(i, **codes_dict["Inpatient Stay"]) and i.dateofService <= uu[i]["IESD"] and uu[i]["IESD"] <= i.dischargeDate:
        uu[i]["IESD_inpatient"] = True

for i in visits:
    if uu[i]["IESD"] != "99999999":
        if 1 in check_for_diagnosis_or_procedures(i, **codes_dict["Bone Mineral Density Tests"]) and get_dates_diff(uu[i]["IESD"], i.dateofService) in range(0, 181):
            numerator.add(i.memberId)
        if 1 in check_for_diagnosis_or_procedures(i, **codes_dict["Osteoporosis Medications"]) and get_dates_diff(uu[i]["IESD"], i.dateofService) in range(0, 181):
            numerator.add(i.memberId)
        if 1 in check_for_diagnosis_or_procedures(i, **codes_dict["Long-Acting Osteoporosis Medications"]) and uu[i]["IESD_inpatient"]:
            numerator.add(i.memberId)

numerator = list(numerator)
print(numerator)
print(len(numerator), len(members))
stars = get_stars_rank(len(numerator), len(members), STARS_RANGES)
print(stars)
