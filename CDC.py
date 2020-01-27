from sqlalchemy import or_, not_, and_, func

from core.helpers import *
from core.models import *

STARS_RANGES_HBA1C = {
    (0, 0.37): 1,
    (0.37, 0.61): 2,
    (0.61, 0.72): 3,
    (0.72, 0.85): 4,
    (0.85, 1): 5,
}
STARS_RANGES_EYE_EXAM = {
    (0, 0.63): 1,
    (0.63, 0.69): 2,
    (0.69, 0.73): 3,
    (0.73, 0.78): 4,
    (0.78, 1): 5,
}
STARS_RANGES_KIDNEY_DISEASE_MONITORING = {
    (0.80, 0.95): 3,
    (0.95, 0.97): 4,
    (0.97, 1): 5,
}
START_AGE = 18
END_AGE = 75
MEASURE_START_YEAR = 2016
MEASURE_END_YEAR = 2016

MEMBERS_START_DATE = "19410101"
MEMBERS_END_DATE = "19981231"

measure = Measure(__file__.split('\\')[-1].split('.')[0])
conn, session = get_database_connection_and_session()


medications = get_medications_codes_list(["Diabetes Medications"])
ace_inhibitors = get_medications_codes_list(["ACE Inhibitor/ARB Medications"])

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
                    "Acute Inpatient": set(),
                    "Outpatient": set(),
                    "Observation": set(),
                    "ED": set(),
                    "Nonacute Inpatient": set(),
                    "Telephone": set(),
                    "Online Assessments": set(),
                    "exclude": {
                        "Acute Inpatient": set(),
                        "Outpatient": set(),
                        "Observation": set(),
                        "ED": set(),
                        "Nonacute Inpatient": set(),
                    },
                    "HbA1c": {
                        "HbA1c Level Less Than 7.0": set(),
                        "HbA1c Level 7.0-9.0": set(),
                        "HbA1c Level Greater Than 9.0": set(),
                        "Other": set(),
                    }
                }
            })

        if check_daterange(i, 2015, 2016):
            acute_inpatient = measure.check_conditions(
                i,
                ["Acute Inpatient", "Diabetes"],
                ["Telehealth Modifier ", "Telehealth POS"]
            )
            if acute_inpatient:
                uu[i.memberId]["Acute Inpatient"].add(acute_inpatient)


            outpatient = measure.check_conditions(
                i,
                ["Outpatient", "Diabetes"],
            )
            if outpatient:
                uu[i.memberId]["Outpatient"].add(outpatient)


            observation = measure.check_conditions(
                i,
                ["Observation", "Diabetes"],
            )
            if observation:
                uu[i.memberId]["Observation"].add(observation)


            ed = measure.check_conditions(
                i,
                ["ED", "Diabetes"],
            )
            if ed:
                uu[i.memberId]["ED"].add(ed)


            nonacute_inpatient = measure.check_conditions(
                i,
                ["Nonacute Inpatient", "Diabetes"],
                ["Telehealth Modifier ", "Telehealth POS"]
            )
            if nonacute_inpatient:
                uu[i.memberId]["Nonacute Inpatient"].add(nonacute_inpatient)
            
            
            telephone = measure.check_conditions(
                i, 
                ["Telephone Visits", "Diabetes"]
            )
            if telephone:
                uu[i.memberId]["Telephone"].add(telephone)


            online_assessment = measure.check_conditions(
                i,
                ["Online Assessments ", "Diabetes"]
            )
            if online_assessment:
                uu[i.memberId]["Online Assessments"].add(online_assessment)


        if check_daterange(i, 2016, 2016):
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
            
            

    pharm = session.query(Pharmacy).filter(
        Pharmacy.ndc_DrugCode.in_(medications),
        Pharmacy.serviceDate.between("20150101", "20161231"),
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
            if len(uu[i]["exclude"]["Outpatient"] | uu[i]["exclude"]["Observation"] | uu[i]["exclude"]["ED"] | uu[i]["exclude"]["Nonacute Inpatient"]) >= 2:
                eligible_members.remove(i)
                continue
            if len(uu[i]["exclude"]["Acute Inpatient"]) >= 1:
                eligible_members.remove(i)
                continue

    eligible_members = exclude_dispenced_dementia(eligible_members)

    return eligible_members





members, visits = get_members_and_visits(MEMBERS_START_DATE, MEMBERS_END_DATE)

eligible_members = get_eligible_members(members, visits)

print(eligible_members)
print(len(eligible_members))

numerator = {
    "HbA1c Other": set(),
    "HbA1c Level Less Than 7.0": set(),
    "HbA1c Level 7.0-9.0": set(),
    "HbA1c Level Greater Than 9.0": set(),
    "Eye Exam": set(),
    "Medical Attention for Nephropathy": set(),
    "BP Control": set(),
}

for i in visits:
    if check_daterange(i, 2016, 2016):
        if measure.check_conditions(i, ["HbA1c Tests"]) != False:
            hba1c_lt_7 = measure.check_conditions(i, ["HbA1c Level Less Than 7.0"])
            if hba1c_lt_7:
                uu[i.memberId]["HbA1c"]["HbA1c Level Less Than 7.0"].add(hba1c_lt_7)
            else:
                hba1c_7_9 = measure.check_conditions(i, ["HbA1c Level 7.0-9.0"])
                if hba1c_7_9:
                    uu[i.memberId]["HbA1c"]["HbA1c Level 7.0-9.0"].add(hba1c_7_9)
                else:
                    hba1c_gt_9 = measure.check_conditions(i, ["HbA1c Level Greater Than 9.0"])
                    if hba1c_gt_9:
                        uu[i.memberId]["HbA1c"]["HbA1c Level Greater Than 9.0"].add(hba1c_gt_9)
                    else:
                        uu[i.memberId]["HbA1c"]["Other"].add(measure.check_conditions(i, ["HbA1c Tests"]))


for i in uu:
    s = sorted(set().union(*uu[i]["HbA1c"].values()), reverse=True)
    if not (s and s[0]):
        continue
    if s[0] in uu[i]["HbA1c"]["HbA1c Level Less Than 7.0"]:
        numerator["HbA1c Level Less Than 7.0"].add(i)
    if s[0] in uu[i]["HbA1c"]["HbA1c Level 7.0-9.0"]:
        numerator["HbA1c Level 7.0-9.0"].add(i)
    if s[0] in uu[i]["HbA1c"]["HbA1c Level Greater Than 9.0"]:
        numerator["HbA1c Level Greater Than 9.0"].add(i)
    if s[0] in uu[i]["HbA1c Other"]:
        numerator["HbA1c Level Greater Than 9.0"].add(i)

hba1c_lt_7_uu = {}
hba1c_exclude = set()
for i in visits:

    if not i.memberId in numerator["HbA1c Level Less Than 7.0"]:
        continue
    if not hba1c_lt_7_uu.get(i):
        hba1c_lt_7_uu.update({
            i.memberId: {
                "CABG": set(),
                "PCI": set(),
                "IVD": {
                    "Outpatient": set(),
                    "Telephone": set(),
                    "Online Assessments": set(),
                    "Acute Inpatient": set(),
                },
                "IVD_Telehealth": {
                    "Outpatient": set(),
                    "Telephone": set(),
                    "Online Assessments": set(),
                }, 
                "Thoratic Aortic Aneurysm": {
                    "Outpatient": set(),
                    "Acute Inpatient": set(),
                }, 
                "Chronic Heart Failure": set(),
                "MI": set(),
                "ESRD": set(),
                "Chronic Kidney Disease": set(),
                "Dementia": set(),
                "Blindless": set(),
                "Amputation": set(),
            }
        })
    if check_daterange(i, 2015, 2016):
        cabg = measure.check_conditions(
            i,
            ["CABG",],
        )
        if cabg:
            hba1c_lt_7_uu[i.memberId]["CABG"].add(cabg)

        pci = measure.check_conditions(
            i,
            ["PCI",],
        )
        if pci:
            hba1c_lt_7_uu[i.memberId]["PCI"].add(pci)

        ivt_outpatient = measure.check_conditions(
            i,
            ["IVT", "Outpatient"],
            ["Telehealth Modifier ", "Telehealth POS"],
        )
        if ivt_outpatient:
            hba1c_lt_7_uu[i.memberId]["IVT"]["Outpatient"].add(ivt_outpatient)

        ivt_telephone = measure.check_conditions(
            i,
            ["IVT", "Telephone"],
            ["Telehealth Modifier ", "Telehealth POS"],
        )
        if ivt_telephone:
            hba1c_lt_7_uu[i.memberId]["IVT"]["Telephone"].add(ivt_telephone)

        ivt_online_assessments = measure.check_conditions(
            i,
            ["IVT", "Online Assessments"],
            ["Telehealth Modifier ", "Telehealth POS"],
        )
        if ivt_online_assessments:
            hba1c_lt_7_uu[i.memberId]["IVT"]["Online Assessments"].add(ivt_online_assessments)

        ivt_acute_inpatient = measure.check_conditions(
            i,
            ["IVT", "Acute Inpatient"],
            ["Telehealth Modifier ", "Telehealth POS"],
        )
        if ivt_acute_inpatient:
            hba1c_lt_7_uu[i.memberId]["IVT"]["Acute Inpatient"].add(ivt_acute_inpatient)

        ivt_outpatient = measure.check_conditions(
            i,
            ["IVT", "Outpatient"],
        #    ["Telehealth Modifier ", "Telehealth POS"],
        )
        if ivt_outpatient:
            hba1c_lt_7_uu[i.memberId]["IVT_Telehealth"]["Outpatient"].add(ivt_outpatient)

        ivt_telephone = measure.check_conditions(
            i,
            ["IVT", "Telephone"],
        #    ["Telehealth Modifier ", "Telehealth POS"],
        )
        if ivt_telephone:
            hba1c_lt_7_uu[i.memberId]["IVT_Telehealth"]["Telephone"].add(ivt_telephone)

        ivt_online_assessments = measure.check_conditions(
            i,
            ["IVT", "Online Assessments"],
        #    ["Telehealth Modifier ", "Telehealth POS"],
        )
        if ivt_online_assessments:
            hba1c_lt_7_uu[i.memberId]["IVT_Telehealth"]["Online Assessments"].add(ivt_online_assessments)

        aneurysm_outpatient = measure.check_conditions(
            i,
            ["Thoratic Aortic Aneurysm", "Outpatient"],
        )
        if aneurysm_outpatient:
            hba1c_lt_7_uu[i.memberId]["Thoratic Aortic Aneurysm"]["Outpatient"].add(aneurysm_outpatient)

        aneurysm_acute_inpatient = measure.check_conditions(
            i,
            ["Thoratic Aortic Aneurysm", "Outpatient"],
            ["Telehealth Modifier ", "Telehealth POS"],
        )
        if aneurysm_acute_inpatient:
            hba1c_lt_7_uu[i.memberId]["Thoratic Aortic Aneurysm"]["Acute Inpatient"].add(aneurysm_acute_inpatient)
        
    chronic_heart_failure = measure.check_conditions(
        i,
        ["Chronic Heart Failure",],
    )
    if chronic_heart_failure:
        hba1c_lt_7_uu[i.memberId]["Chronic Heart Failure"].add(chronic_heart_failure)

    mi = measure.check_conditions(
        i,
        ["MI",],
    )
    if mi:
        hba1c_lt_7_uu[i.memberId]["MI"].add(mi)

    esrd = measure.check_conditions(
        i,
        ["ESRD",],
        ["Telehealth Modifier ", "Telehealth POS"],
    )
    if esrd:
        hba1c_lt_7_uu[i.memberId]["ESRD"].add(esrd)

    esrd_obsolete = measure.check_conditions(
        i,
        ["ESRD Obsolete",],
        ["Telehealth Modifier ", "Telehealth POS"],
    )
    if esrd_obsolete:
        hba1c_lt_7_uu[i.memberId]["ESRD"].add(esrd_obsolete)

    chronic_kidney_disease = measure.check_conditions(
        i,
        ["CKD Stage 4",],
    )
    if chronic_kidney_disease:
        hba1c_lt_7_uu[i.memberId]["Chronic Kidney Disease"].add(chronic_kidney_disease)

    dementia = measure.check_conditions(
        i,
        ["Dementia",],
    )
    if dementia:
        hba1c_lt_7_uu[i.memberId]["Dementia"].add(dementia)

    dementia_frontotemporal = measure.check_conditions(
        i,
        ["Frontotemporal Dementia",],
    )
    if dementia_frontotemporal:
        hba1c_lt_7_uu[i.memberId]["Dementia"].add(dementia_frontotemporal)

    blindless = measure.check_conditions(
        i,
        ["Blindless",],
    )
    if blindless:
        hba1c_lt_7_uu[i.memberId]["Blindless"].add(blindless)

    amputation = measure.check_conditions(
        i,
        ["Lower Extremity Amputation",],
    )
    if amputation:
        hba1c_lt_7_uu[i.memberId]["Amputation"].add(amputation)

    first_checker_set = set().union(
        hba1c_lt_7_uu[i.memberId]["Amputation"],
        hba1c_lt_7_uu[i.memberId]["Blindless"],
        hba1c_lt_7_uu[i.memberId]["Dementia"],
        hba1c_lt_7_uu[i.memberId]["Chronic Kidney Disease"],
        hba1c_lt_7_uu[i.memberId]["ESRD"],
        hba1c_lt_7_uu[i.memberId]["MI"],
        hba1c_lt_7_uu[i.memberId]["Chronic Heart Failure"],
        hba1c_lt_7_uu[i.memberId]["CABG"],
        hba1c_lt_7_uu[i.memberId]["PCI"],
        hba1c_lt_7_uu[i.memberId]["Thoratic Aortic Aneurysm"]["Outpatient"],
        hba1c_lt_7_uu[i.memberId]["Thoratic Aortic Aneurysm"]["Acute Inpatient"],
    )

    if len(first_checker_set):
        hba1c_exclude.add(i.memberId)

    second_checker_set = set().union(
        hba1c_lt_7_uu[i.memberId]["IVT"]["Outpatient"],
        hba1c_lt_7_uu[i.memberId]["IVT"]["Telephone"],
        hba1c_lt_7_uu[i.memberId]["IVT"]["Online Assessments"],
        hba1c_lt_7_uu[i.memberId]["IVT"]["Acute Inpatient"],
    )

    second_checker_set_telehealth = set().union(
        hba1c_lt_7_uu[i.memberId]["IVT_Telehealth"]["Outpatient"],
        hba1c_lt_7_uu[i.memberId]["IVT_Telehealth"]["Telephone"],
        hba1c_lt_7_uu[i.memberId]["IVT_Telehealth"]["Online Assessments"],
    )

    flag_first_year = False
    flag_second_year = False
    for j in second_checker_set:
        if j.startswith("2015"):
            flag_first_year = True
        if j.startswith("2016"):
            flag_second_year = True

    if flag_first_year and flag_second_year:
        hba1c_exclude.add(i.memberId)

    if flag_first_year and not flag_second_year:
        for j in second_checker_set_telehealth:
            if j.startswith('2016'):
                hba1c_exclude.add(i.memberId)
                break

    if not flag_first_year and flag_second_year:
        for j in second_checker_set_telehealth:
            if j.startswith('2015'):
                hba1c_exclude.add(i.memberId)
                break



for i in hba1c_exclude:
    if i in numerator["HbA1c Level Less Than 7.0"]:
        numerator["HbA1c Level Less Than 7.0"].remove(i)


eye_exam_uu = {}

for i in visits:
    if not i.memberId in eligible_members:
        continue
    if not eye_exam_uu.get(i.memberId):
        eye_exam_uu.update({
            i.memberId: {
                "Diabetic Retinal Screening With Eye Care Professional": False,
                "Unilateral Eye Enucleation": set(),
            }
        })
    if check_daterange(i, 2015, 2015):
        diabetic_retinal_screening_negative = measure.check_conditions(
            i,
            ["Diabetic Retinal Screening Negative"],
        )
        if diabetic_retinal_screening_negative:
            numerator["Eye Exam"].add(i.memberId)

        diabetes_mellitus_without_complications = measure.check_conditions(
            i,
            ["Diabetic Retinal Screening", "Diabetes Mellitus Without Complications"],
        )
        if diabetes_mellitus_without_complications:
            numerator["Eye Exam"].add(i.memberId)

        diabetic_retinal_screening_with_eye_care_professional = measure.check_conditions(
            i,
            ["Diabetic Retinal Screening With Eye Care Professional"]
        )
        if diabetic_retinal_screening_with_eye_care_professional:
            eye_exam_uu[i.memberId]["Diabetic Retinal Screening With Eye Care Professional"] = True

    if check_daterange(i, 2016, 2016):
        diabetic_retinal_screening = measure.check_conditions(
            i,
            ["Diabetic Retinal Screening"],
        )
        if diabetic_retinal_screening:
            numerator["Eye Exam"].add(i.memberId)

        diabetic_retinal_screening_with_eye_care_professional = measure.check_conditions(
            i,
            ["Diabetic Retinal Screening With Eye Care Professional"]
        )
        if diabetic_retinal_screening_with_eye_care_professional:
            numerator["Eye Exam"].add(i.memberId)

        diabetic_retinal_screening_negative = measure.check_conditions(
            i,
            ["Diabetic Retinal Screening Negative"],
        )
        if diabetic_retinal_screening_negative:
            numerator["Eye Exam"].add(i.memberId)

    unilateral_eye_enucleation = measure.check_conditions(
        i,
        ["Unilateral Eye Enucleation", "Bilateral Modifier"],
    )
    if unilateral_eye_enucleation:
        numerator["Eye Exam"].add(i.memberId)

    two_unilateral_eye_enucleations = measure.check_conditions(
        i,
        ["Unilateral Eye Enucleation"]
    )
    if two_unilateral_eye_enucleations:
        eye_exam_uu[i.memberId]["Unilateral Eye Enucleation"].add((i.dateofService, 'UEE'))

    unilateral_eye_enucleations_left = measure.check_conditions(
        i,
        ["Unilateral Eye Enucleation Left"]
    )
    if unilateral_eye_enucleations_left:
        eye_exam_uu[i.memberId]["Unilateral Eye Enucleation"].add((i.dateofService, 'Left'))

    unilateral_eye_enucleations_right = measure.check_conditions(
        i,
        ["Unilateral Eye Enucleation Right"]
    )
    if unilateral_eye_enucleations_right:
        eye_exam_uu[i.memberId]["Unilateral Eye Enucleation"].add((i.dateofService, 'Right'))


for i in eye_exam_uu:
    eye_exam_uu[i]["Unilateral Eye Enucleation"] = sorted(eye_exam_uu[i]["Unilateral Eye Enucleation"], key=lambda x: x[0])
    for j in eye_exam_uu[i]["Unilateral Eye Enucleation"]:
        for k in eye_exam_uu[i]["Unilateral Eye Enucleation"]:
            if j[0] < k[0]:
                if get_dates_diff(j[0], k[0]) >= 14:
                    if j[1] == "UEE" or k[1] == "UEE":
                        numerator["Eye Exam"].add(i.memberId)
            if j[1] == "Right" and k[1] == "Left" or j[1] == "Left" and k[1] == "Right":
                numerator["Eye Exam"].add(i.memberId)

for i in visits:
    if not i.memberId in eligible_members:
        continue
    #if not eye_exam_uu.get(i.memberId):
    #    eye_exam_uu.update({
    #        i.memberId: {
    #            "Diabetic Retinal Screening With Eye Care Professional": False,
    #            "Unilateral Eye Enucleation": set(),
    #        }
    #    })
    if check_daterange(i, 2016, 2016):
        urine_protein_tests = measure.check_conditions(
            i,
            ["Urine Protein Tests"],
        )
        if urine_protein_tests:
            numerator["Medical Attention for Nephropathy"].add(i.memberId)

        nephropathy_treatment = measure.check_conditions(
            i,
            ["Nephropathy Treatment"],
        )
        if nephropathy_treatment:
            numerator["Medical Attention for Nephropathy"].add(i.memberId)

        chronic_kidney_disease = measure.check_conditions(
            i,
            ["CKD Stage 4"],
        )
        if chronic_kidney_disease:
            numerator["Medical Attention for Nephropathy"].add(i.memberId)

        esrd = measure.check_conditions(
            i,
            ["ESRD"],
            ["Telehealth Modifier ", "Telehealth POS"]
        )
        if esrd:
            numerator["Medical Attention for Nephropathy"].add(i.memberId)

        kidney_transplant = measure.check_conditions(
            i,
            ["Kidney Transplant"],
        )
        if kidney_transplant:
            numerator["Medical Attention for Nephropathy"].add(i.memberId)

        if i.providerID in ["NEP001", "NEP002", "DOB001", "DOB002"]:
            numerator["Medical Attention for Nephropathy"].add(i.memberId)

pharm = session.query(Pharmacy).filter(
    Pharmacy.ndc_DrugCode.in_(ace_inhibitors),
    Pharmacy.serviceDate.between("20160101", "20161231"),
)
for i in pharm:
    if i.memberId in eligible_members:
        numerator["Medical Attention for Nephropathy"].add(i.memberId)

bp_uu = {}
for i in visits:
    if not i.memberId in eligible_members:
        continue
    if not bp_uu.get(i.memberId):
        bp_uu.update({
            i.memberId: ''
        })
    if check_daterange(i, 2016, 2016):
        bp_outpatient_140_80 = measure.check_conditions(
            i,
            ["Outpatient", "Systolic Less Than 140", "Diastolic Less Than 80"],
        )
        if bp_outpatient_140_80:
            bp_uu[i.memberId] = max(bp_uu[i.memberId], bp_outpatient_140_80)

        bp_outpatient_140_90 = measure.check_conditions(
            i,
            ["Outpatient", "Systolic Less Than 140", "Diastolic 80-89"],
        )
        if bp_outpatient_140_90:
            bp_uu[i.memberId] = max(bp_uu[i.memberId], bp_outpatient_140_90)

        bp_nonacute_inpatient_140_80 = measure.check_conditions(
            i,
            ["Nonacute Inpatient", "Systolic Less Than 140", "Diastolic Less Than 80"],
        )
        if bp_nonacute_inpatient_140_80:
            bp_uu[i.memberId] = max(bp_uu[i.memberId], bp_nonacute_inpatient_140_80)

        bp_nonacute_inpatient_140_90 = measure.check_conditions(
            i,
            ["Nonacute Inpatient", "Systolic Less Than 140", "Diastolic 80-89"],
        )
        if bp_nonacute_inpatient_140_90:
            bp_uu[i.memberId] = max(bp_uu[i.memberId], bp_nonacute_inpatient_140_90)

        bp_rm_140_80 = measure.check_conditions(
            i,
            ["Remote Blood Pressure Monitoring", "Systolic Less Than 140", "Diastolic Less Than 80"],
        )
        if bp_rm_140_80:
            bp_uu[i.memberId] = max(bp_uu[i.memberId], bp_rm_140_80)

        bp_rm_140_90 = measure.check_conditions(
            i,
            ["Remote Blood Pressure Monitoring", "Systolic Less Than 140", "Diastolic 80-89"],
        )
        if bp_rm_140_90:
            bp_uu[i.memberId] = max(bp_uu[i.memberId], bp_rm_140_90)
                
for i in bp_uu:
    if bp_uu[i]:
        numerator["BP Control"].add(i)

for i in numerator:
    print(i, len(numerator[i]), numerator[i])


for i in visits:
    if i.member_age >= 66 and i.memberId in eligible_members:
        frailty_advanced = measure.check_conditions(
            i,
            ["Frailty", "Advanced Illness "]
        )
        if frailty_advanced and i.memberId in numerator["HbA1c Level Greater Than 9.0"]:
            #eligible_members.pop(i.memberId)
            numerator["HbA1c Level Greater Than 9.0"].pop(i.memberId)

print(get_stars_rank(numerator["HbA1c Level Greater Than 9.0"], eligible_members, STARS_RANGES_HBA1C, True))
print(get_stars_rank(numerator["Eye Exam"], eligible_members, STARS_RANGES_EYE_EXAM))
print(get_stars_rank(numerator["Medical Attention for Nephropathy"], eligible_members, STARS_RANGES_KIDNEY_DISEASE_MONITORING))
