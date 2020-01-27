from sqlalchemy import or_, not_, and_, func

from core.helpers import *
from core.models import *

STARS_RANGES = {
    (0, 0.5): 1,
    (0.5, 0.66): 2,
    (0.66, 0.76): 3,
    (0.76, 0.83): 4,
    (0.83, 1): 5,
}
START_AGE = 52
END_AGE = 74
MEASURE = __file__.split('\\')[-1].split('.')[0]
MEASURE_START_YEAR = 2017
MEASURE_END_YEAR = 2018

conn, session = get_database_connection_and_session()
codes_dict = get_codes_for_measure(MEASURE)



for i in codes_dict.items():
    print(i)



def get_all_members():
    gm = session.query(GeneralMembership).filter(
        and_(
            GeneralMembership.gender == 'F',
            GeneralMembership.dateOfBirth >= "{}0101".format(MEASURE_END_YEAR - END_AGE),
            GeneralMembership.dateOfBirth <= "{}1231".format(MEASURE_END_YEAR - START_AGE)
        )
    )
    members = {i.memberId: i for i in gm}

    return exclude_dispenced_dementia(members)


def get_all_visits(members):
    visits = session.query(Visit).filter(
        Visit.memberId.in_(list(members.keys())),
        Visit.dateofService.between(
            "{}0101".format(MEASURE_START_YEAR),
            "{}1231".format(MEASURE_END_YEAR),
        ),
    ).order_by(Visit.memberId, Visit.dateofService).all()
    for i in visits:
        age = (int(members[i.memberId].dateOfBirth) - int(i.dateofService)) // 10000
        setattr(i, "member_age", age)
    return visits



def get_eligible_members(members, visits):
    exclude_members_ids = []

    uu = {}
    for i in visits:
        if i.memberId in exclude_members_ids:
            continue
        if not uu.get(i.memberId):
            uu.update({
                i.memberId: {
                    "Frailty": 0,
                    "Outpatient": 0,
                    "Observation": 0,
                    "ED": 0,
                    "Nonacute Inpatient": 0,
                    "Advanced Illness": 0,
                }
            })

        d1 = check_for_diagnosis_or_procedures(i, **codes_dict["Frailty"])
        d2 = check_for_diagnosis_or_procedures(i, **codes_dict["Outpatient"])
        d3 = check_for_diagnosis_or_procedures(i, **codes_dict["Observation"])
        d4 = check_for_diagnosis_or_procedures(i, **codes_dict["ED"])
        d5 = check_for_diagnosis_or_procedures(i, **codes_dict["Nonacute Inpatient"])
        d6 = check_for_diagnosis_or_procedures(i, **codes_dict["Advanced Illness "])
        uu[i.memberId]["Frailty"] += 1 if 1 in d1.values() else 0
        uu[i.memberId]["Outpatient"] += 1 if 1 in d2.values() else 0
        uu[i.memberId]["Observation"] += 1 if 1 in d3.values() else 0
        uu[i.memberId]["ED"] += 1 if 1 in d4.values() else 0
        uu[i.memberId]["Nonacute Inpatient"] += 1 if 1 in d5.values() else 0
        uu[i.memberId]["Advanced Illness"] += 1 if 1 in d6.values() else 0
        if 1 in d6.values() and 1 in check_for_diagnosis_or_procedures(i, **codes_dict["Acute Inpatient"]) and i.member_age >= 66:
            exclude_members_ids.append(i.memberId)

        for j in uu[i.memberId].values():
            if j > 1 and uu[i.memberId]["Advanced Illness"] > 1 and i.member_age >= 66:
                exclude_members_ids.append(i.memberId)

    for i in exclude_members_ids:
        if members.get(i):
            members.pop(i)
    return members







members = get_all_members()
visits = get_all_visits(members)

members = get_eligible_members(members, visits)

numerator = set()

for i in visits:
    print(i)
    if 1 in check_for_diagnosis_or_procedures(i, **codes_dict["Mammography"]):
        numerator.add(i.memberId)
numerator = list(numerator)
print(numerator)
print(len(numerator), len(members))
stars = get_stars_rank(len(numerator), len(members), STARS_RANGES)
print(stars)
