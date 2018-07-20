require_relative 'ojdbc6.jar'
require 'csv'
require 'base64'
require 'sequel'

class Hackathon

  def initialize
    ###PreProd
    @db = Sequel.connect('jdbc:oracle:thin:@clms06ract.nwie.net:1521/clms01ch.nsc.net', :user => 'clm_adapterdb', :password => Base64.decode64('Q2FwM2VwcmQy\n'))
    ##Breakfix
    # @db = Sequel.connect('jdbc:oracle:thin:@clms05ract.nwie.net:1521/clms02q.nsc.net', :user => 'clm_adapterdb', :password => Base64.decode64('Q2FiM2VrZml4Mg==\n'))
  end

  def get_claims
    claim_numbers = @db.fetch("select claimnumber from CLM_ADAPTERDB.CLAIMS_NOTES where model_type = 'TEST'")
    claims = []
    claim_numbers.all.each do |record|
      claims<<record[:claimnumber]
    end
    claims
  end

  def get_assigned_user(claim)
    query = @db.fetch("select contact.firstname, contact.lastname
    from claimcenter.cc_claim claim
    inner join claimcenter.cc_user usr on claim.assigneduserid = usr.id
    inner join claimcenter.cc_contact contact on usr.contactid = contact.id
    where claim.claimnumber = '#{claim}'").first
    query[:firstname] + ' ' + query[:lastname]
  end
  def get_claiminfo(claim)
    @db.fetch("select claim.claimnumber, policy.accountnumber, contact.name,policy.policynumber,claim.lossdate,claim.agencyid, tl_state.typecode as riskstate,policy.producercode, contact.firstname, contact.lastname, contact.name
  from claimcenter.cc_address address
  inner join claimcenter.cc_contact contact ON contact.primaryaddressid = address.id
  inner join claimcenter.cc_claimcontact cc on contact.id = cc.contactid
  inner join claimcenter.cc_claim claim on cc.claimid = claim.id
  inner join claimcenter.cc_policy policy on claim.policyid = policy.id
  inner join claimcenter.cc_claimcontactrole ccrole on ccrole.claimcontactid = CC.ID
  inner join claimcenter.cctl_contactrole croletl on ccrole.role = croletl.id
  inner join claimcenter.cctl_state tl_state on tl_state.id = address.state
  inner join claimcenter.cctl_contactrole croletl on ccrole.role = croletl.id
  where claim.claimnumber = '#{claim}' and croletl.typecode = 'insured'").first
  end

  def get_agentinfo(claim)
    @db.fetch("select contact.FirstName,contact.HomePhone, contact.LastName,  contact.WorkPhone, contact.name
    from claimcenter.cc_claim claim
    inner join claimcenter.cc_claimcontact cc on claim.id = cc.claimid
    inner join claimcenter.cc_contact contact on contact.id = cc.contactid
    inner join claimcenter.cc_claimcontactrole ccrole on ccrole.claimcontactid = cc.id
    inner join claimcenter.cctl_contactrole conrole on conrole.id = ccrole.role
    where claim.claimnumber = '#{claim}' and conrole.typecode = 'agent'").first
  end

  def get_losslocationinfo(claim)
    @db.fetch("select address.ADDRESSLINE1, tl_state.typecode as STATE, address.POSTALCODE
    from CLAIMCENTER.CC_ADDRESS address
    inner join claimcenter.cc_claim claim on claim.losslocationid = address.id
    inner join claimcenter.cctl_state tl_state on tl_state.id = address.state
    where CLAIMNUMBER = '#{claim}'").first
  end

  def get_totalpaid(claim)
    @db.fetch("select claimrpt.totalpayments
    from claimcenter.cc_claimrpt claimrpt
    inner join claimcenter.cc_claim claim on claim.id = claimrpt.claimid
    where claim.claimnumber = '#{claim}'").first[:totalpayments].to_f
  end

  def get_risk_types(claim)
    precision = @db.fetch("select model_type, claim_ml_result_2 from CLM_ADAPTERDB.CLAIMS_NOTES WHERE claimnumber = '#{claim}'").first
    if !precision[:claim_ml_result_2].nil? and precision[:claim_ml_result_2].to_f > 0.90
      case precision[:model_type]
        when 'PROP'
          'Vacancy Occupancy'
        when 'WCMP'
          'Light Duty'
        else
          ''
      end
    else
      ''
    end
  end

  def define_risk_types(risk)
    long_names = []
    risk.split(',').each do |stuff|
      case stuff.downcase
        when 'pexd'
          long_names<<'Property Exterior Damage'
        when 'rfdm'
          long_names<<'Roof Damage'
        when 'vowr'
          long_names<<'Vacancy Occupancy'
      end
    end
    long_names.join(',').gsub('"','')
  end

  def update_db_risk_table(claim,risktype)
    @db.run("UPDATE CLM_ADAPTERDB.CLAIMS_NOTES set CLAIM_ML_RESULT_3 = '#{risktype}' where CLAIMNUMBER = '#{claim}'") unless risktype.empty?
  end
end

puts "Starting at #{Time.now}"
hackathon = Hackathon.new

claims = hackathon.get_claims

claims.each do |claim|

  ###Read the CLaims Notes Table
  risktype = hackathon.get_risk_types(claim)
  next if risktype.empty?
  ###Get General Claim Information
  claiminfo = hackathon.get_claiminfo(claim)
  ###Get AssignedUser
  assigneduser = hackathon.get_assigned_user(claim)
  ###Get Agent Info
  agent = hackathon.get_agentinfo(claim)
  ###Get Loss Location
  losslocation = hackathon.get_losslocationinfo(claim)

  ###Uncomment this when you want to update the table
  hackathon.update_db_risk_table(claim,risktype)

  ###Write to CSV
  CSV.open("riskfile.csv", "ab") do |csv|
    csv << [claiminfo[:claimnumber], risktype, 'Personal Lines',claiminfo[:accountnumber],(claiminfo[:name].nil? ? claiminfo[:firstname] + ' ' + claiminfo[:lastname] : claiminfo[:name]),claiminfo[:policynumber],Time.now.strftime("%m/%d/%Y"),claiminfo[:lossdate].strftime("%m/%d/%Y"),assigneduser, '555.867.5309',claiminfo[:producercode],'91-Home Office',claiminfo[:riskstate],agent.nil? ? '' : agent[:name].gsub('"',''),losslocation[:addressline1],losslocation[:state],losslocation[:postalcode],['Priority 1','Priority 2','Priority 3'].sample] unless risktype.empty?
  end

end

puts "Ending at #{Time.now}"