from sqlalchemy import or_, not_, and_, func

from core.helpers import *
from core.models import *

STARS_RANGES_MEDICATION_LIST = {
    (0, 0.63): 1,
    (0.63, 0.77): 2,
    (0.77, 0.87): 3,
    (0.87, 0.95): 4,
    (0.95, 1): 5,
}
STARS_RANGES_FUNCTIONAL_ASSESMENT = {
    (0, 0.55): 1,
    (0.55, 0.71): 2,
    (0.71, 0.85): 3,
    (0.85, 0.93): 4,
    (0.93, 1): 5,
}
STARS_RANGES_PAIN_ASSESMENT = {
    (0, 0.55): 1,
    (0.55, 0.71): 2,
    (0.71, 0.85): 3,
    (0.85, 0.93): 4,
    (0.93, 1): 5,
}
START_AGE = 66
END_AGE = 200
MEASURE = __file__.split('\\')[-1].split('.')[0]
MEASURE_START_YEAR = 2018
MEASURE_END_YEAR = 2018

conn, session = get_database_connection_and_session()
codes_dict = get_codes_for_measure(MEASURE)



for i in codes_dict.items():
    print(i)



def get_all_members():
    gm = session.query(GeneralMembership).filter(
        and_(
            GeneralMembership.dateOfBirth >= "{}0101".format(MEASURE_END_YEAR - END_AGE),
            GeneralMembership.dateOfBirth <= "{}1231".format(MEASURE_END_YEAR - START_AGE)
        )
    )
    members = {i.memberId: i for i in gm}

    return exclude_dispenced_dementia(members)


def get_all_visits(members, start=MEASURE_START_YEAR, end=MEASURE_END_YEAR):
    visits = session.query(Visit).filter(
        Visit.memberId.in_(list(members.keys())),
        Visit.dateofService.between(
            "{}0101".format(start),
            "{}1231".format(end),
        ),
    ).order_by(Visit.memberId, Visit.dateofService).all()
    for i in visits:
        age = (int(members[i.memberId].dateOfBirth) - int(i.dateofService)) // 10000
        setattr(i, "member_age", age)
    return visits




members = get_all_members()
visits = get_all_visits(members)

numerator = {
    "Advance Care Planning": set(),
    "Medication Review": set(),
    "Functional Status Assessment": set(),
    "Pain Assessment": set(),   
    "Medication List": set(),
}


for i in visits:
    if 1 in check_for_diagnosis_or_procedures(i, **codes_dict["Advance Care Planning"]):
        numerator["Advance Care Planning"].add(i.memberId)
for i in visits:
    if 1 in check_for_diagnosis_or_procedures(i, **codes_dict["Medication Review"]) and \
        1 in check_for_diagnosis_or_procedures(i, **codes_dict["Medication List"]):
            numerator["Medication Review"].add(i.memberId)
    if 1 in check_for_diagnosis_or_procedures(i, **codes_dict["Transitional Care Management Services"]) and \
        (not 1 in check_for_diagnosis_or_procedures(i, **codes_dict["Acute Inpatient"]) or not 1 in check_for_diagnosis_or_procedures(i, **codes_dict["Acute Inpatient POS"])):
            numerator["Medication Review"].add(i.memberId)
for i in visits:
    if 1 in check_for_diagnosis_or_procedures(i, **codes_dict["Functional Status Assessment"]) and \
        (not 1 in check_for_diagnosis_or_procedures(i, **codes_dict["Acute Inpatient"]) or not 1 in check_for_diagnosis_or_procedures(i, **codes_dict["Acute Inpatient POS"])):
            numerator["Functional Status Assessment"].add(i.memberId)
for i in visits:
    if 1 in check_for_diagnosis_or_procedures(i, **codes_dict["Pain Assessment"]) and \
        (not 1 in check_for_diagnosis_or_procedures(i, **codes_dict["Acute Inpatient"]) or not 1 in check_for_diagnosis_or_procedures(i, **codes_dict["Acute Inpatient POS"])):
            numerator["Pain Assessment"].add(i.memberId)


for i in visits:
    if 1 in check_for_diagnosis_or_procedures(i, **codes_dict["Medication List"]):
        numerator["Medication List"].add(i.memberId)
print(len(numerator["Medication List"]))
stars = get_stars_rank(len(numerator["Medication List"]), len(members), STARS_RANGES_MEDICATION_LIST)
print(stars)


for i in visits:
    if 1 in check_for_diagnosis_or_procedures(i, **codes_dict["Functional Status Assessment"]):
        numerator["Functional Status Assessment"].add(i.memberId)
print(len(numerator["Functional Status Assessment"]))
stars = get_stars_rank(len(numerator["Functional Status Assessment"]), len(members), STARS_RANGES_FUNCTIONAL_ASSESMENT)
print(stars)


for i in visits:
    if 1 in check_for_diagnosis_or_procedures(i, **codes_dict["Pain Assessment"]):
        numerator["Pain Assessment"].add(i.memberId)
print(len(numerator["Pain Assessment"]))
stars = get_stars_rank(len(numerator["Pain Assessment"]), len(members), STARS_RANGES_PAIN_ASSESMENT)
print(stars)