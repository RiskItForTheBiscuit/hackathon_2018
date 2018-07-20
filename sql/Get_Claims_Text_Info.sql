DECLARE
    claims_text             CLOB := NULL;
    clm_id                  INTEGER := NULL;         
    clm_number              VARCHAR2(10) := null;
    policy_type             VARCHAR(50) := NULL;
    uw_company              VARCHAR(50) := NULL;
    clm_description         VARCHAR2(2000) := NULL;
    activity_subject        VARCHAR2(255) := NULL;
    activity_description    LONG := NULL;
    note_subject            VARCHAR2(255) := NULL;
    note_body               CLOB := NULL;
    rec_count              INTEGER := 0;
    model_type              CHAR(4) := NULL;
    insured_main_contact    CHAR(1) := NULL;
    

    CURSOR Claims_Cur
     IS
        SELECT DISTINCT clm.id AS id
                , clm.claimnumber AS clmno
                , FIL.FREQUENCY as model_type 
                , UT.TYPECODE as UW_COMPANY 
                , PT.TYPECODE as POLICY_TYPE
             FROM CRI_FILTER fil 
             join claimcenter.cc_claim clm
               ON fil.CLAIMNUMBER = clm.CLAIMNUMBER
             join CLAIMCENTER.CC_POLICY pol
               on pol.ID = clm.POLICYID 
              join CLAIMCENTER.CCTL_POLICYTYPE pt
                on pt.id = pol.POLICYTYPE
              join CLAIMCENTER.CCTL_UNDERWRITINGCOMPANYTYPE ut 
                on UT.ID = POL.UNDERWRITINGCO
             where FREQUENCY = 'WCNW';  
           --fil.CLAIMNUMBER NOT IN ('330835-GD', '157878-GG');
            --AND fil.CLAIMNUMBER = '080643-GG';
            
   CURSOR Activity_Cur
     IS
        SELECT REGEXP_REPLACE(lower(a.SUBJECT),'[^a-zA-Z'']', ' ')
               ,REGEXP_REPLACE(lower(a.DESCRIPTION),'[^a-zA-Z'']', ' ')
 

             FROM CC_ACTIVITY a
            WHERE a.CLAIMID = clm_id;    


   CURSOR Notes_Cur
     IS
        SELECT REGEXP_REPLACE(lower(n.SUBJECT),'[^a-zA-Z'']', ' ')
               ,REGEXP_REPLACE(lower(n.body),'[^a-zA-Z'']', ' ')
 

             FROM CC_NOTE n
            WHERE n.CLAIMID = clm_id 
              AND LOWER(n.subject) not like '%risk%';
 
              
               
 BEGIN
     OPEN Claims_Cur;
     DBMS_OUTPUT.ENABLE(1000000); 
     
     LOOP
        FETCH Claims_Cur INTO clm_id, clm_number, model_type, uw_company,policy_type;
        
        EXIT WHEN Claims_Cur%NOTFOUND;
        
        claims_text := NULL;
        insured_main_contact := 'N';
       
        BEGIN 
                SELECT max('Y') 
                  INTO insured_main_contact
                  FROM CLAIMCENTER.CC_CLAIM c
                        JOIN CLAIMCENTER.CC_CLAIMCONTACT cci
                          ON cci.CLAIMID = c.ID 
                        JOIN CLAIMCENTER.CC_CLAIMCONTACTROLE cori
                          ON cori.CLAIMCONTACTID = cci.ID 
                        JOIN CLAIMCENTER.CC_contact coi 
                         ON coi.ID = cci.CONTACTID 
                       JOIN CLAIMCENTER.CCTL_CONTACTROLE rti
                         ON rti.ID = cori.ROLE                           
                        AND rti.TYPECODE = 'maincontact'
                 WHERE c.id = clm_id 
                 AND coi.ID = c.INSUREDDENORMID;

            EXCEPTION 
                WHEN NO_DATA_FOUND THEN
                   insured_main_contact := 'N'; 

         END;

        
        
        --dbms_output.put_line('ClaimNumber=' || clm_number);
        
        SELECT REGEXP_REPLACE(lower(DESCRIPTION),'[^a-zA-Z'']', ' ')
          INTO clm_description
          FROM CC_CLAIM
          WHERE ID = clm_id;
        
        --dbms_output.put_line('Description=' || clm_description);
        claims_text := claims_text || clm_number  || ' ' || clm_description;
        --dbms_output.put_line('<Description>' || clm_description || '</Description>'); 

        OPEN Activity_Cur;
        LOOP 
            activity_subject := ''; 
            activity_description := '';
            FETCH Activity_Cur into activity_subject, activity_description;
            EXIT WHEN Activity_Cur%NOTFOUND;
            --dbms_output.put_line(activity_subject ||' ' || activity_description);       
            claims_text := claims_text || ' ' || activity_subject  || ' ' || activity_description;
            --dbms_output.put_line('<Text> act: Subject -> ' || activity_subject || ' Body -> ' || activity_description || '</Text>'); 
            
        END LOOP;        
        
        OPEN Notes_Cur;
        LOOP 
            note_subject := ''; 
            note_body := '';            
            FETCH Notes_Cur into note_subject, note_body;
            EXIT WHEN Notes_Cur%NOTFOUND;
 
            claims_text := claims_text || ' ' || note_subject  || ' ' || note_body;
            --dbms_output.put_line('<Text> note: Subject -> ' || note_subject || ' Body -> ' || note_body || '</Text>'); 
            --dbms_output.put_line(note_subject ||' ' || note_body);   
            
        END LOOP;
        
        CLOSE Activity_Cur;
        CLOSE Notes_Cur;
        
        DELETE FROM CLM_ADAPTERDB.CLAIMS_NOTES WHERE CLAIMNUMBER = clm_number;  
            
       -- IF SQL%NOTFOUND THEN
       --    DBMS_OUTPUT.PUT_LINE(clm_number || 'not found in notes table');
       -- END IF;
 
        
        INSERT INTO CLM_ADAPTERDB.CLAIMS_NOTES (CLAIMNUMBER, POLICY_TYPE, UW_COMPANY, NOTES, MODEL_TYPE, INSURED_IS_MAIN_CONTACT) 
          VALUES (clm_number, policy_type, uw_company, claims_text, model_type, insured_main_contact); 
            
      --  DELETE FROM BUCLM_ADAPTERDB.CRI_FILTER WHERE CLAIMNUMBER = clm_number;
        
      --  IF SQL%NOTFOUND THEN
      --     DBMS_OUTPUT.PUT_LINE(clm_number || 'not found in filer table');
      --  END IF;           
                             
        
        
        

        
        --dbms_output.put_line(claims_text);
        --SELECT LENGTH(claims_text) into str_length from DUAL;
        --dbms_output.put_line(str_length);
        --dbms_output.put_line(claims_text);
        --dbms_output.put_line('<Description>' || clm_description || '</Description>'); 
        
        rec_count := rec_count + 1;
        
        IF MOD(rec_count, 10000) = 0 THEN
           dbms_output.put_line('commit count at : ' ||rec_count );
           COMMIT;
        END IF;
        
     END LOOP;
     
     dbms_output.put_line('commit count at : ' ||rec_count );
     COMMIT;
     
END;
     
