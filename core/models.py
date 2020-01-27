from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()



class GeneralMembership(Base):
    __tablename__ = 'bcs_generalmembershipstage'

    id = Column(Integer, primary_key=True)
    memberId = Column(String, primary_key=True)
    gender = Column(String)
    dateOfBirth = Column(String)
    memberLastName = Column(String)
    memberFirstName = Column(String)
    memberMiddleInitial = Column(String)
    subscriberOrFamilyIdNumber = Column(String)
    mailingAddress1 = Column(String)
    mailingAddress2 = Column(String)
    city = Column(String)
    state = Column(String)
    zip = Column(String)
    telephonePhone = Column(String)
    parent_CaretakerFirstName = Column(String)
    parent_CaretakerLastName = Column(String)
    parent_CaretakerMiddleInitial = Column(String)
    race = Column(String)
    ethnicity = Column(String)
    raceDataSource = Column(String)
    ethnDataSource = Column(String)
    spokenLanguage = Column(String)
    spokenLanguageSource = Column(String) 
    writtenLanguage = Column(String)
    writtenLanguageSource = Column(String) 
    otherLanguage = Column(String)
    otherLanguageSource = Column(String)
    

    def __repr__(self):
        return "<GeneralMembership(memberId='%s', dateOfBirth='%s')>" % (self.memberId, self.dateOfBirth)




class MembershipEnrollment(Base):
    __tablename__ = 'bcs_membershipenrollmentstage'

    id = Column(Integer, primary_key=True)
    memberId = Column(String, primary_key=True) 
    startDate = Column(String) 
    disenrollmentDate = Column(String) 
    dentalBenefit = Column(String) 
    drugBenefit = Column(String) 
    mentalHealthBenefitInpatient = Column(String) 
    mentalHealthBenefitIntensiveOutpatient = Column(String) 
    mentalHealthBenefitOutpatient_ED = Column(String) 
    chemDepBenefitInpatient = Column(String) 
    chemDepBenefitIntensiveOutpatient = Column(String) 
    chemDepBenefitOutpatient_ED = Column(String) 
    hospiceBenefit = Column(String) 
    lti = Column(String) 
    payer = Column(String)
    healthPlanEmployeeFlag = Column(String) 
    indicator = Column(String)    


    def __repr__(self):
        return "<MembershipEnrollment(memberId='%s')>" % (self.memberId)



class Pharmacy(Base):
    __tablename__ = 'bcs_pharmacystage'

    id = Column(Integer, primary_key=True)
    memberId = Column(String) 
    daysSupply = Column(String)
    serviceDate = Column(String)
    ndc_DrugCode = Column(String)
    claimStatus = Column(String) 
    metricQuantity = Column(String) 
    quantityDispensed = Column(String) 
    supplementalData = Column(String) 
    providerNPI = Column(String) 
    pharmacyNPI = Column(String)


    def __repr__(self):
        return "<Pharmacy(memberId='%s', NDC_DrugCode='%s')>" % (self.memberId, self.ndc_DrugCode)


class Provider(Base):
    __tablename__ = 'bcs_providerstage'

    id = Column(Integer, primary_key=True)
    providerId = Column(String)
    pcp = Column(String) 
    obg_yn = Column(String) 
    mh_Provider = Column(String) 
    eyeCareProvider = Column(String) 
    dentist = Column(String) 
    nephrologist = Column(String) 
    anesthesiologist = Column(String) 
    npr_Provider = Column(String) 
    pas_Provider = Column(String) 
    providerPrescribingPrivileges = Column(String) 
    clinicalPharmacist = Column(String) 
    hospital = Column(String) 
    snf = Column(String) 
    surgeon = Column(String) 
    registeredNurse = Column(String)


    def __repr__(self):
        return "<Provider(providerId='%s')>" % (self.providerID)



class Visit(Base):
    __tablename__ = 'bcs_visitstage'

    id = Column(Integer, primary_key=True)
    memberId = Column(String) 
    dateofService = Column(String) 
    admissionDate = Column(String) 
    dischargeDate = Column(String) 
    coveredDays = Column(String) 
    cpt = Column(String) 
    cptModifier1 = Column(String) 
    cptModifier2 = Column(String) 
    hcpcs = Column(String) 
    cptII = Column(String) 
    cptIIModifier = Column(String) 
    principalICDDiagnosis = Column(String) 
    icd_Diagnosis_2 = Column(String) 
    icd_Diagnosis_3 = Column(String) 
    icd_Diagnosis_4 = Column(String) 
    icd_Diagnosis_5 = Column(String) 
    icd_Diagnosis_6 = Column(String) 
    icd_Diagnosis_7 = Column(String) 
    icd_Diagnosis_8 = Column(String) 
    icd_Diagnosis_9 = Column(String) 
    icd_Diagnosis_10 = Column(String) 
    principalICDProcedure = Column(String) 
    icd_Procedure_2 = Column(String) 
    icd_Procedure_3 = Column(String) 
    icd_Procedure_4 = Column(String) 
    icd_Procedure_5 = Column(String) 
    icd_Procedure_6 = Column(String) 
    icd_Identifier = Column(String)
    drg = Column(String) 
    dischargeStatus = Column(String) 
    ub_Revenue = Column(String)
    ub_TypeOfBill = Column(String)
    numberOfTimes = Column(String) 
    cms_PlaceOfService = Column(String) 
    claimStatus = Column(String) 
    providerID = Column(String)
    supplementalData = Column(String)


    def __repr__(self):
        return "<Visit(memberId='%s', dateofService='%s')>" % (self.memberId, self.dateofService)
