from datetime import datetime
from sqlalchemy import create_engine, or_, not_, and_, func
from sqlalchemy.orm import Session
import pandas as pd

from .models import *


def get_database_connection_and_session():
    engine = create_engine('mysql+mysqldb://root:12345678@localhost:3306/hedisdevdb', pool_recycle=3600)
    conn = engine.connect()
    session = Session(engine)
    return conn, session

conn, session = get_database_connection_and_session()


def get_datetime_from_str(string):
    return datetime(
        year=int(string[:4]),
        month=int(string[4:6]),
        day=int(string[6:])
    )


def get_dates_diff(start, end):
    start = get_datetime_from_str(start)
    end = get_datetime_from_str(end)
    return (end - start).days


def get_age(date_of_birth, anchor_date):
    if date_of_birth.endswith('0229'):
        date_of_birth = date_of_birth[:4] + '0301'
    return (int(anchor_date) - int(date_of_birth)) // 10000


def get_members_and_visits(start, end):
    gm = session.query(GeneralMembership).filter(
        and_(
            GeneralMembership.dateOfBirth >= start,
            GeneralMembership.dateOfBirth <= end,
        )
    )
    members = {i.memberId: i for i in gm}
    visits = session.query(Visit).filter(
        Visit.memberId.in_(list(members.keys())),
    ).order_by(Visit.memberId, Visit.dateofService).all()
    for i in visits:
        age = get_age(members[i.memberId].dateOfBirth, i.dateofService if i.dateofService else "20181231")
        setattr(i, "member_age", age)
    return members, visits


def check_daterange(obj, start_year, end_year, start_day="0101", end_day="1231"):
    if obj.dateofService:
        if str(start_year) + start_day <= obj.dateofService and obj.dateofService <= str(end_year) + end_day:
            return True
    return False



class Measure(object):
    def __init__(self, measure):
        filename = "xls/%s.xlsx" % measure
        measures_to_value_sets_sheet_name = "%s Measures to Value Sets" % measure
        value_sets_to_codes_sheet_name = "%s Value Sets to Codes" % measure

        xl_file = pd.ExcelFile(filename)

        dfs = {sheet_name: xl_file.parse(sheet_name) for sheet_name in xl_file.sheet_names}

        value_set_oids = []

        for i, r in dfs[measures_to_value_sets_sheet_name].iterrows():
            if r["Measure ID"] == measure:
                value_set_oids.append(r["Value Set OID"])

        codes_pairs = []
        for i, r in dfs[value_sets_to_codes_sheet_name].iterrows():
            if r["Value Set OID"] in value_set_oids:
                codes_pairs.append([r["Code"], r["Code System"], r["Definition"], r["Value Set Name"]])

        self.__codes_dict = {}
        for i in codes_pairs:
            if i[1] == "CPT-CAT-II":
                i[1] = "CPTII"
            if i[1] == "CPT Modifier":
                i[1] = "CPTMOD"
            if not self.__codes_dict.get(i[3]):
                self.__codes_dict.update({i[3]: {}})
            if not self.__codes_dict[i[3]].get(i[1]):
                self.__codes_dict[i[3]].update({i[1]: []})
            self.__codes_dict[i[3]][i[1]].append(i[0])


    def check_for_diagnosis_or_procedures(self, obj, CPT=None, ICD10CM=None, ICD9CM=None, ICD9PCS=None, ICD10PCS=None, UBREV=None, HCPCS=None, LOINC=None, CPTII=None, POS=None, CPTMOD=None, UBTOB=None):
        data = {
            "CPT": 0,
            "ICD10CM": 0,
            "ICD9CM": 0,
            "ICD9PCS": 0,
            "ICD10PCS": 0,
            "UBREV": 0,
            "UBTOB": 0,
            "HCPCS": 0,
            "LOINC": 0,
            "POS": 0,
        }
        if CPT:
            if obj.cpt in CPT:
                data["CPT"] = 1
        if CPTII:
            if obj.cptII in CPTII:
                data["CPT"] = 1
        if CPTMOD:
            if obj.cptModifier1 in CPTMOD or obj.cptModifier2 in CPTMOD:
                data["CPT"] = 1
        if ICD10CM:
            if obj.principalICDDiagnosis in ICD10CM:
                data["ICD10CM"] = 1
            for i in dir(obj):
                if i.startswith("icd_Diagnosis_") and getattr(obj, i) in ICD10CM:
                    data["ICD10CM"] = 1
        if ICD9CM:
            if obj.principalICDDiagnosis in ICD9CM:
                data["ICD9CM"] = 1
            for i in dir(obj):
                if i.startswith("icd_Diagnosis_") and getattr(obj, i) in ICD9CM:
                    data["ICD9CM"] = 1
        if ICD9PCS:
            if obj.principalICDProcedure in ICD9PCS:
                data["ICD9PCS"] = 1
            for i in dir(obj):
                if i.startswith("icd_Procedure_") and getattr(obj, i) in ICD9PCS:
                    data["ICD9PCS"] = 1
        if ICD10PCS:
            if obj.principalICDProcedure in ICD10PCS:
                data["ICD9PCS"] = 1
            for i in dir(obj):
                if i.startswith("icd_Procedure_") and getattr(obj, i) in ICD10PCS:
                    data["ICD9PCS"] = 1
        if UBREV:
            if obj.ub_Revenue in UBREV:
                data["UBREV"] = 1
        if UBTOB:
            if obj.ub_TypeOfBill in UBTOB:
                data["UBTOB"] = 1
        if HCPCS:
            if obj.hcpcs in HCPCS:
                data["HCPCS"] = 1
        return data


    def check_conditions(self, obj, enabled_types=[], disabled_types=[]):
        for i in enabled_types:
            if max(self.check_for_diagnosis_or_procedures(obj, **self.__codes_dict[i]).values()) == 0:
                return False
        for i in disabled_types:
            if max(self.check_for_diagnosis_or_procedures(obj, **self.__codes_dict[i]).values()) != 0:
                return False
        return obj.dateofService


def get_medications_codes_list(medication_types):
    xl_file = pd.ExcelFile("xls/Medications List.xlsx")
    dfs = {sheet_name: xl_file.parse(sheet_name) for sheet_name in xl_file.sheet_names}
    medications = []
    for i, r in dfs["Medications List to NDC Codes"].iterrows():
        if r["Medication List"] in medication_types:
            medications.append(str(r["NDC Code"]))
    return medications


def get_stars_rank(numerator, denominator, stars_ranges, substract_from_100=False):
    result = 1 if substract_from_100 else 2 * len(numerator) / len(denominator) - len(numerator) / len(denominator)
    for i in stars_ranges:
        if result >= i[0] and result < i[1]:
            return stars_ranges[i]


def exclude_dispenced_dementia(members, ages=(66, 200)):
    xl_file = pd.ExcelFile("xls/Dementia Medications List.xlsx")
    dfs = {sheet_name: xl_file.parse(sheet_name) for sheet_name in xl_file.sheet_names}
    dementia_medications = []
    for i, r in dfs["Dementia"].iterrows():
        if r["Generic_Drug_Name"].split()[0] in ["Donepezil", "Galantamine", "Rivastigmine", "Memantine"]:
            dementia_medications.append(str(r["NDC"]))


    exclude_dementia_disp = []
    pharm = session.query(Pharmacy).filter(
        Pharmacy.ndc_DrugCode.in_(dementia_medications)
    )
    for i in pharm:
        age = (int(members[i.memberId].dateOfBirth) - int(i.serviceDate)) // 10000
        if age >= ages[0] and age <= ages[1]:
            exclude_dementia_disp.append(i.memberId)
    
    for i in exclude_dementia_disp:
        if i in members:
            members.remove(i)

    return members


