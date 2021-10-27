-- BI-1956 product mapping

WITH cte_product AS (
    SELECT
        m.product_uuid
         , m.product_key
         , m.product_name
         , m.product_note
         , m.product_length_and_payment
         , m.product_family
         , m.is_recurring
         , m.is_subsequent_recurring
         , p.product_is_shippable
         , p.product_is_for_sale
         , p.product_price_net
         , p.product_weight
         , p.product_tax
         , (p.product_created_ts AT TIME ZONE 'Europe/Berlin')::DATE AS product_created_dt
    FROM dwh_main.umd_product_map m
             JOIN dwh_main.dim_product p
                  ON p.product_uuid = m.product_uuid
)
   , customer_product AS (
    SELECT
        c.customer_id
         , ctr.contract_id
         , upm.product_uuid AS current_product_uuid
         , upm.product_name AS current_product_name
         , upm.product_created_dt AS current_product_created_dt
         , upm.product_note AS current_product_note
         , upm.product_price_net AS current_product_price_net
         , CASE WHEN upm.product_price_net = p.max_price_net THEN TRUE ELSE FALSE END AS current_price_is_max_price_for_the_product
         , upm2.product_uuid AS future_product_uuid
         , upm2.product_name AS future_product_name
         , upm2.product_created_dt AS future_product_created_dt
         , upm2.product_note AS future_product_note
         , upm2.product_price_net AS future_product_price_net
    FROM dwh_main.dim_customer c
             JOIN dwh_main.dim_contract ctr
                  ON ctr.customer_uuid = c.customer_uuid
             JOIN dwh_main.fact_contract_line fcl
                  ON ctr.contract_key = fcl.contract_key
             JOIN cte_product upm
                  ON upm.product_key = fcl.contract_item_product_key
             LEFT JOIN cte_product upm2
                       ON upm2.product_name = upm.product_name
                           AND upm2.product_length_and_payment = upm.product_length_and_payment
                           AND upm2.is_recurring = upm.is_recurring
                           AND upm2.is_subsequent_recurring = upm.is_subsequent_recurring
                           AND upm2.product_is_shippable = upm.product_is_shippable
                           AND upm2.product_is_for_sale = upm.product_is_for_sale
                           AND upm2.product_weight = upm.product_weight
                           AND upm2.product_uuid <> upm.product_uuid
                           AND upm2.product_price_net > upm.product_price_net
                           AND upm2.product_tax <> 0.16
             LEFT JOIN
         (SELECT product_name, MAX(product_price_net) AS max_price_net
          FROM cte_product
          GROUP BY product_name) p
         ON p.product_name = upm.product_name
    WHERE fcl.opcode <> 'D'
      AND upm.product_family != 'Other'
--     AND ctr.contract_id = 'V12127102'
--     AND upm.product_uuid = 'ade12889-23be-40fa-895a-52cd7b8e0891'
      AND ctr.contract_id IN (
                              'V70765101',
                              'V77119385',
                              'V10155631',
                              'V71675129',
                              'V61300650',
                              'V33893452',
                              'V89153237',
                              '68302657',
                              'V10069311',
                              'V82702057',
                              'V38491554',
                              'V10199483',
                              'V95357364',
                              '18667405',
                              'V71828549',
                              'V10895783',
                              'V83924016',
                              'V61048102',
                              'V15339356',
                              'V40827159',
                              'V42786782',
                              'V74733458',
                              '98665807',
                              'V45099908',
                              'V89328241',
                              'V64379397',
                              'V52865716',
                              'V14301046',
                              'V98339024',
                              '93572683',
                              'V26099507',
                              '37830987',
                              'V40007051',
                              '72235318',
                              'V13414382',
                              '10689027',
                              '81703701',
                              '36645101',
                              'V33494333',
                              '46854770',
                              '49887195',
                              '77051166',
                              '33668059',
                              '87656836',
                              'V59199443',
                              '49098177',
                              '96523358',
                              'V21901721',
                              'V60568785',
                              '81521465',
                              'V54357466',
                              'V79122834',
                              '83843180',
                              'V63911474',
                              'V74345918',
                              'V90166226',
                              'V14677760',
                              'V83986388',
                              '26544656',
                              '58809271',
                              '81030202',
                              'V27559760',
                              'V92753459',
                              '88287982',
                              'V22678008',
                              'V87173461',
                              '96405923',
                              'V95204952',
                              '91169705',
                              'V18289906',
                              'V25276464',
                              'V25298019',
                              'V28253672',
                              'V34086360',
                              'V40612985',
                              'V51466357',
                              'V68799759',
                              'V88668252',
                              '23570466',
                              'V24906092',
                              'V51148385',
                              '38313173',
                              'V16618940',
                              '50504030',
                              '68006426',
                              'V18459446',
                              'V34233471',
                              'V67385004',
                              'V84967249',
                              'V48847085',
                              '27700133',
                              'V14906676',
                              'V68027385',
                              '40814036',
                              'V16537728',
                              'V19484656',
                              '21120561',
                              '28853260',
                              '28880880',
                              '71482869',
                              '86135945',
                              '92154209',
                              '96496481',
                              'V54834731',
                              'V61082860',
                              'V81180154',
                              'V83213566',
                              'V84321310',
                              'V87395966',
                              'V90550579',
                              'V92844495',
                              'V93043648',
                              'V97039169',
                              'V99984885',
                              'V33984291',
                              'V14351925',
                              'V51316904',
                              '99520792',
                              'V32073366',
                              '58827350',
                              '74038155',
                              'V49237493',
                              'V64798131',
                              'V71284419',
                              'V72842754',
                              'V73886748',
                              'V75494160',
                              'V82601201',
                              'V32839897',
                              'V38995442',
                              'V48239711',
                              'V55519224',
                              'V78870891',
                              'V94166241',
                              'V70653072',
                              '68077006',
                              '46301588',
                              '62143880',
                              'V25173659',
                              'V99403704',
                              '46625378',
                              '63527826',
                              'V12348982',
                              'V13734952',
                              'V26366574',
                              'V50919232',
                              'V81266382',
                              'V89190446',
                              'V91568739',
                              '80903217',
                              '78519761',
                              'V28559023',
                              'V40295672',
                              '36832778',
                              '86250054',
                              '19742703',
                              'V64671309',
                              'V83415258',
                              'V98324465',
                              '35059913',
                              '60831658',
                              '66756851',
                              '70097920',
                              '78828076',
                              '88286337',
                              'V10217765',
                              'V14231608',
                              'V14481010',
                              'V15546249',
                              'V17907511',
                              'V21461837',
                              'V28574924',
                              'V29870342',
                              'V30720922',
                              'V41288156',
                              'V46937727',
                              'V47797946',
                              'V51620022',
                              'V52478606',
                              'V53479673',
                              'V56032147',
                              'V58795043',
                              'V64199821',
                              'V70102335',
                              'V70487830',
                              'V72983549',
                              'V73724225',
                              'V78074978',
                              'V78241760',
                              'V78702682',
                              'V85921366',
                              'V90204964',
                              'V91554955',
                              'V94705140',
                              'V96022990',
                              'V99080395',
                              'V99769023',
                              '10657306',
                              'V37150049',
                              'V71098344',
                              'V90178395',
                              '53522885',
                              'V58704469',
                              'V68087292',
                              'V96012670',
                              '15831587',
                              '24974676',
                              'V32909995',
                              'V90791574',
                              '11711634',
                              '29685576',
                              'V10998054',
                              'V18635792',
                              'V19370092',
                              'V26072153',
                              'V29181270',
                              'V29920773',
                              'V43567390',
                              'V69610272',
                              'V74937530',
                              'V97359169',
                              'V75753039',
                              'V76346129',
                              '42071479',
                              '49223125',
                              '52583941',
                              'V38301090',
                              'V40807760',
                              'V47824157',
                              'V50875986',
                              'V28239371',
                              'V65804076',
                              '93848831',
                              '80785864',
                              'V27917123',
                              'V28984530',
                              'V83995816'
        )
)
--    SELECT * FROM customer_product; -- CHECK CUSTOMERS
   , cte_1 AS (SELECT DISTINCT
                   current_product_uuid
                             , current_product_name
                             , current_product_created_dt
                             , current_product_note
                             , current_product_price_net
                             , current_price_is_max_price_for_the_product
/*
IMPORTANT NOTE:
1. When running mapping for B2C uncomment B2C mapping and comment out the B2B part.
2. When running mapping for B2B uncomment B2B mapping and comment out the B2C part.
*/                             , CASE
        -- B2C mapped in October https://docs.google.com/spreadsheets/d/1_iiImHOuVW6OjESbl5z9g5jXr0IziKGHgJ9PQ0Vqs8g/edit#gid=278718204
--                                    WHEN current_product_uuid = '4914d433-88cd-4b2e-bbbe-39624e2d7bc6' THEN '061928ea-042b-4182-a1fe-a711f5134389'
--                                    WHEN current_product_uuid = '7fb84572-05ac-4c94-ae9b-f3fcf07c2cf5' THEN '061928ea-042b-4182-a1fe-a711f5134389'
--                                    WHEN current_product_uuid = '83d52040-6f87-428c-83f8-32eb853b6d22' THEN 'b16a5957-b5d1-4b2f-860b-331c25fe2989'
--                                    WHEN current_product_uuid = 'c0e8ec1d-9f72-4309-a377-f5ca0508644f' THEN '0a64e2b9-406c-4810-a3d2-c9d67dc95b89'
--                                    WHEN current_product_uuid = 'efcab066-7216-4948-a765-99d82ba44566' THEN '061928ea-042b-4182-a1fe-a711f5134389'
        -- B2B mapped in October https://docs.google.com/spreadsheets/d/1_iiImHOuVW6OjESbl5z9g5jXr0IziKGHgJ9PQ0Vqs8g/edit#gid=278718204
                                   WHEN current_product_uuid = '0c809251-a8d4-4d54-a681-41839f022ef5' THEN '65a0bfd5-d789-4a7e-aad2-9787d1f1048e'
                                   WHEN current_product_uuid = '1c2891ea-2d8d-464b-8f3a-9d22f2ed42d0' THEN '7d6fb064-5df7-4858-b543-bf4cd612897a'
                                   WHEN current_product_uuid = '2ee2f831-999c-44b9-a6ca-66da41bfbfdb' THEN '47d0bd5b-fdaa-492e-90d7-20df81940aea'
                                   WHEN current_product_uuid = '3b94b261-f47a-4393-9faf-a298543664ac' THEN '65a0bfd5-d789-4a7e-aad2-9787d1f1048e'
                                   WHEN current_product_uuid = '44f7e2e7-8e5d-4604-a107-56a10e31a021' THEN 'f0622671-3256-41b0-9a49-aee469973257'
                                   WHEN current_product_uuid = '4914d433-88cd-4b2e-bbbe-39624e2d7bc6' THEN '65a0bfd5-d789-4a7e-aad2-9787d1f1048e'
                                   WHEN current_product_uuid = '68c0319d-36ca-4731-af05-4d9ff589d99b' THEN '83bc9af9-cc35-4d7e-ab84-e49fc1cea89d'
                                   WHEN current_product_uuid = '726bc39b-c3c1-48f0-8544-c391427eaad5' THEN '552a3e33-c99b-49db-a01c-fe949e73508a'
                                   WHEN current_product_uuid = '7fb84572-05ac-4c94-ae9b-f3fcf07c2cf5' THEN '65a0bfd5-d789-4a7e-aad2-9787d1f1048e'
                                   WHEN current_product_uuid = '8a85301e-df82-437f-9b96-11eb46420ae4' THEN 'dedbe9ed-b0c7-4746-814f-97bbd1c5ba69'
                                   WHEN current_product_uuid = 'c0e8ec1d-9f72-4309-a377-f5ca0508644f' THEN 'f0622671-3256-41b0-9a49-aee469973257'
                                   WHEN current_product_uuid = 'cee04f53-d490-4c5e-ad43-ca76f5533b5f' THEN '8c864718-f6cf-4cd9-8fda-71b7aed4fec6'
                                   WHEN current_product_uuid = 'd4bab2dd-1dd4-466e-8a26-3ac5ba984eb0' THEN 'e9ff0a5f-fb44-4b2b-8a05-72896f09bdf5'
                                   WHEN current_product_uuid = 'f2fffb32-6a5a-4a67-908b-cdc52f09fee9' THEN '9b892ebc-6f1a-4cab-a95e-cac364fd6d49'
        -- B2B mapped in November https://docs.google.com/spreadsheets/d/1_iiImHOuVW6OjESbl5z9g5jXr0IziKGHgJ9PQ0Vqs8g/edit#gid=482325070
                                   WHEN current_product_uuid = '408a0153-5567-452c-a10e-242f6fabaaf4' THEN '5fc29394-7dcb-4535-9641-5dfbdc503f80'
                                   WHEN current_product_uuid = 'ade12889-23be-40fa-895a-52cd7b8e0891' THEN 'f0622671-3256-41b0-9a49-aee469973257'
                                   ELSE future_product_uuid
        END AS future_product_uuid
                             , future_product_name
                             , future_product_created_dt
                             , future_product_note
                             , future_product_price_net
               FROM customer_product cp)
SELECT DISTINCT
    current_product_uuid
              , current_product_name
              , current_product_created_dt
              , current_product_note
              , current_product_price_net
              , current_price_is_max_price_for_the_product
              , future_product_uuid
              , coalesce(cp.product_name, future_product_name) AS future_product_name  -- product mapped in the past has priority
              , coalesce(cp.product_created_dt, future_product_created_dt) AS future_product_created_dt
              , coalesce(cp.product_note, future_product_note) AS future_product_note
              , coalesce(cp.product_price_net, future_product_price_net) AS future_product_price_net
FROM cte_1 c1
         LEFT JOIN cte_product cp
                   ON cp.product_uuid = c1.future_product_uuid
ORDER BY current_product_uuid
;

