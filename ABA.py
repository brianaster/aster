from sqlalchemy import or_, not_, and_, func

from core.helpers import *
from core.models import *

STARS_RANGES = {
    (0, 0.78): 1,
    (0.78, 0.92): 2,
    (0.92, 0.96): 3,
    (0.96, 0.99): 4,
    (0.99, 1): 5,
}
START_AGE = 18
END_AGE = 74
MEASURE = __file__.split('\\')[-1].split('.')[0]
MEASURE_START_YEAR = 2017
MEASURE_END_YEAR = 2018

conn, session = get_database_connection_and_session()
code_dict = get_codes_for_measure(MEASURE)



for i in code_dict.items():
    print(i)



def get_all_members():
    gm = session.query(GeneralMembership).filter(
        and_(
            GeneralMembership.dateOfBirth >= "{}0101".format(MEASURE_END_YEAR - END_AGE),
            GeneralMembership.dateOfBirth <= "{}1231".format(MEASURE_START_YEAR - START_AGE)
        )
    )
    return {i.memberId: i for i in gm}

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
                    "Pregnancy": 0,
                }
            })

        result = check_for_diagnosis_or_procedures(i, **code_dict["Pregnancy"])
        uu[i.memberId]["Pregnancy"] += 1 if 1 in result.values() else 0
        if uu[i.memberId]["Pregnancy"] and i.member_age >= 20:
            exclude_members_ids.append(i.memberId)

    for i in exclude_members_ids:
        if members.get(i):
            members.pop(i)

    exclude_members_ids = []

    for i in visits:
        result = check_for_diagnosis_or_procedures(i, **code_dict["Outpatient"])
        if not 1 in result.values():
            exclude_members_ids.append(i.memberId)

    for i in exclude_members_ids:
        if members.get(i):
            members.pop(i)

    return members, visits




members = get_all_members()
visits = get_all_visits(members)

members, visits = get_eligible_members(members, visits)


numerator = set()

members_after_20 = []
members_before_20 = []

for i in visits:
    if i.member_age >= 20 and members.get(i.memberId):
        members_after_20.append(i.memberId)
        if 1 in check_for_diagnosis_or_procedures(i, **code_dict["BMI"]):
            numerator.add(i.memberId)
    if i.member_age < 20 and members.get(i.memberId):
        members_after_20.append(i.memberId)
        if 1 in check_for_diagnosis_or_procedures(i, **code_dict["BMI Percentile"]):
            numerator.add(i.memberId)


print(len(members_before_20) + len(members_after_20))
numerator = list(numerator)
print(numerator)
print(len(numerator), len(members_before_20) + len(members_after_20))
stars = get_stars_rank(len(numerator), len(members_after_20) + len(members_before_20), STARS_RANGES)
print(stars)