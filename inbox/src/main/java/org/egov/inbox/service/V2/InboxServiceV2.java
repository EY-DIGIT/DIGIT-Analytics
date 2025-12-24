package org.egov.inbox.service.V2;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.wnameless.json.flattener.JsonFlattener;
import com.google.gson.Gson;
import com.jayway.jsonpath.JsonPath;
import lombok.extern.slf4j.Slf4j;
import org.egov.hash.HashService;
import org.egov.inbox.config.InboxConfiguration;
import org.egov.inbox.repository.ServiceRequestRepository;
import org.egov.inbox.repository.builder.V2.InboxQueryBuilder;
import org.egov.inbox.service.V2.validator.ValidatorDefaultImplementation;
import org.egov.inbox.service.WorkflowService;
import org.egov.inbox.util.MDMSUtil;
import org.egov.inbox.web.model.Inbox;
import org.egov.inbox.web.model.InboxRequest;
import org.egov.inbox.web.model.InboxResponse;
import org.egov.inbox.web.model.V2.*;
import org.egov.inbox.web.model.workflow.BusinessService;
import org.egov.inbox.web.model.workflow.ProcessInstance;
import org.egov.inbox.web.model.workflow.ProcessInstanceSearchCriteria;
import org.egov.tracer.model.CustomException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

import java.util.*;

import static org.egov.inbox.util.InboxConstants.*;

@Service
@Slf4j
public class InboxServiceV2 {

    @Autowired
    private InboxConfiguration config;

    @Autowired
    private InboxQueryBuilder queryBuilder;

    @Autowired
    private ServiceRequestRepository serviceRequestRepository;

    @Autowired
    private WorkflowService workflowService;

    @Autowired
    private ValidatorDefaultImplementation validator;

    @Autowired
    private MDMSUtil mdmsUtil;

    @Autowired
    private ObjectMapper mapper;

    @Autowired
    private HashService hashService;


    /**
     *
     * @param inboxRequest
     * @return
     */
    public InboxResponse getInboxResponse(InboxRequest inboxRequest){

        validator.validateSearchCriteria(inboxRequest);
        InboxQueryConfiguration inboxQueryConfiguration = mdmsUtil.getConfigFromMDMS(inboxRequest.getInbox().getTenantId(), inboxRequest.getInbox().getProcessSearchCriteria().getModuleName());
        hashParamsWhereverRequiredBasedOnConfiguration(inboxRequest.getInbox().getModuleSearchCriteria(), inboxQueryConfiguration);
        List<Inbox> items = getInboxItems(inboxRequest, inboxQueryConfiguration.getIndex());
        enrichProcessInstanceInInboxItems(items);
      //  items.removeIf(item -> item.getProcessInstance() == null);
        Integer totalCount = CollectionUtils.isEmpty(inboxRequest.getInbox().getProcessSearchCriteria().getStatus()) ? 0 : getTotalApplicationCount(inboxRequest, inboxQueryConfiguration.getIndex());
        List<HashMap<String, Object>> statusCountMap = CollectionUtils.isEmpty(inboxRequest.getInbox().getProcessSearchCriteria().getStatus()) ? new ArrayList<>() : getStatusCountMap(inboxRequest, inboxQueryConfiguration.getIndex());
        Integer nearingSlaCount = CollectionUtils.isEmpty(inboxRequest.getInbox().getProcessSearchCriteria().getStatus()) ? 0 : getApplicationsNearingSlaCount(inboxRequest, inboxQueryConfiguration.getIndex());
        InboxResponse inboxResponse = InboxResponse.builder().items(items).totalCount(totalCount).statusMap(statusCountMap).nearingSlaCount(nearingSlaCount).build();

        return inboxResponse;
    }

    private void hashParamsWhereverRequiredBasedOnConfiguration(Map<String, Object> moduleSearchCriteria, InboxQueryConfiguration inboxQueryConfiguration) {

        inboxQueryConfiguration.getAllowedSearchCriteria().forEach(searchParam -> {
            if(!ObjectUtils.isEmpty(searchParam.getIsHashingRequired()) && searchParam.getIsHashingRequired()){
                if(moduleSearchCriteria.containsKey(searchParam.getName())){
                    if(moduleSearchCriteria.get(searchParam.getName()) instanceof List){
                        List<Object> hashedParams = new ArrayList<>();
                        ((List<?>) moduleSearchCriteria.get(searchParam.getName())).forEach(object -> {
                            hashedParams.add(hashService.getHashValue(object));
                        });
                        moduleSearchCriteria.put(searchParam.getName(), hashedParams);
                    }else{
                        Object hashedValue = hashService.getHashValue(moduleSearchCriteria.get(searchParam.getName()));
                        moduleSearchCriteria.put(searchParam.getName(), hashedValue);
                    }
                }
            }
        });
    }

    private void enrichProcessInstanceInInboxItems(List<Inbox> items) {
        /*
          As part of the new inbox, having currentProcessInstance as part of the index is mandated. This has been
          done to avoid having redundant network calls which could hog the performance.
        */
        if(items != null) {
            log.info("Enriching process instances for {} inbox items", items.size());
            items.forEach(item -> {
                if(item.getBusinessObject() != null && item.getBusinessObject().containsKey(CURRENT_PROCESS_INSTANCE_CONSTANT)) {
                    // Set process instance object in the native process instance field declared in the model inbox class.
                    ProcessInstance processInstance = mapper.convertValue(item.getBusinessObject().get(CURRENT_PROCESS_INSTANCE_CONSTANT), ProcessInstance.class);
                    item.setProcessInstance(processInstance);

                    // Remove current process instance from business object in order to avoid having redundant data in response.
                    item.getBusinessObject().remove(CURRENT_PROCESS_INSTANCE_CONSTANT);
                }
            });
        }
    }


    private List<Inbox> getInboxItems(InboxRequest inboxRequest, String indexName){
        List<BusinessService> businessServices = workflowService.getBusinessServices(inboxRequest);
        enrichActionableStatusesFromRole(inboxRequest, businessServices);
        if(CollectionUtils.isEmpty(inboxRequest.getInbox().getProcessSearchCriteria().getStatus())){
            return new ArrayList<>();
        }
        Map<String, Object> finalQueryBody = queryBuilder.getESQuery(inboxRequest, Boolean.TRUE);
        try {
            String q = mapper.writeValueAsString(finalQueryBody);
            log.info("Query: "+q);
        }
        catch (Exception e){
            e.printStackTrace();
        }
        StringBuilder uri = getURI(indexName, SEARCH_PATH);
        Object result = serviceRequestRepository.fetchESResult(uri, finalQueryBody);
        List<Inbox> inboxItemsList = parseInboxItemsFromSearchResponse(result, businessServices);
        log.info(result.toString());
        return inboxItemsList;
    }

    private void enrichActionableStatusesFromRole(InboxRequest inboxRequest, List<BusinessService> businessServices) {
        ProcessInstanceSearchCriteria processCriteria = inboxRequest.getInbox().getProcessSearchCriteria();
        String tenantId = inboxRequest.getInbox().getTenantId();
        processCriteria.setTenantId(tenantId);

        HashMap<String, String> StatusIdNameMap = workflowService.getActionableStatusesForRole(inboxRequest.getRequestInfo(), businessServices,
                inboxRequest.getInbox().getProcessSearchCriteria());
        log.info(StatusIdNameMap.toString());
        List<String> actionableStatusUuid = new ArrayList<>();
        if (StatusIdNameMap.values().size() > 0) {
            if (!CollectionUtils.isEmpty(processCriteria.getStatus())) {
                processCriteria.getStatus().forEach(statusUuid -> {
                    if(StatusIdNameMap.keySet().contains(statusUuid)){
                        actionableStatusUuid.add(statusUuid);
                    }
                });
                inboxRequest.getInbox().getProcessSearchCriteria().setStatus(actionableStatusUuid);
            } else {
                inboxRequest.getInbox().getProcessSearchCriteria().setStatus(new ArrayList<>(StatusIdNameMap.keySet()));
            }
        }else{
            inboxRequest.getInbox().getProcessSearchCriteria().setStatus(new ArrayList<>());
        }
    }

    public Integer getTotalApplicationCount(InboxRequest inboxRequest, String indexName){

        Map<String, Object> finalQueryBody = queryBuilder.getESQuery(inboxRequest, Boolean.FALSE);
        StringBuilder uri = getURI(indexName, COUNT_PATH);
        Map<String, Object> response = (Map<String, Object>) serviceRequestRepository.fetchESResult(uri, finalQueryBody);
        Integer totalCount = 0;
        if(response.containsKey(COUNT_CONSTANT)){
            totalCount = (Integer) response.get(COUNT_CONSTANT);
        }else{
            throw new CustomException("INBOX_COUNT_ERR", "Error occurred while executing ES count query");
        }
        return totalCount;
    }

    public List<HashMap<String, Object>> getStatusCountMap(InboxRequest inboxRequest, String indexName){
        Map<String, Object> finalQueryBody = queryBuilder.getStatusCountQuery(inboxRequest);
        StringBuilder uri = getURI(indexName, SEARCH_PATH);
        Map<String, Object> response = (Map<String, Object>) serviceRequestRepository.fetchESResult(uri, finalQueryBody);
        Set<String> actionableStatuses = new HashSet<>(inboxRequest.getInbox().getProcessSearchCriteria().getStatus());
        HashMap<String, Object> statusCountMap = parseStatusCountMapFromAggregationResponse(response, actionableStatuses);
        List<HashMap<String, Object>> transformedStatusMap = transformStatusMap(inboxRequest, statusCountMap);
        return transformedStatusMap;
    }

   private Long getApplicationServiceSla(Map<String, Long> businessServiceSlaMap,
                                      Map<String, Long> stateUuidSlaMap,
                                      Object data) {

    if (data == null) {
        log.warn("Data object is null while calculating SLA");
        return null;
    }

    Long currentDate = System.currentTimeMillis(); // current time
    Map<String, Object> auditDetails = (Map<String, Object>) ((Map<String, Object>) data).get(AUDIT_DETAILS_KEY);
    if (auditDetails == null) {
        log.warn("Audit details are null for data: {}", data);
        return null;
    }

    String stateUuid = null;
    String businessService = null;

    // Safely read stateUuid
    try {
        stateUuid = JsonPath.read(data, STATE_UUID_PATH);
    } catch (com.jayway.jsonpath.PathNotFoundException e) {
        log.warn("State UUID not found in data: {}", data);
    }

    // Safely read businessService
    try {
        businessService = JsonPath.read(data, BUSINESS_SERVICE_PATH);
    } catch (com.jayway.jsonpath.PathNotFoundException e) {
        log.warn("Business Service not found in data: {}", data);
    }

    // SLA based on stateUuid
    if (stateUuid != null && stateUuidSlaMap.containsKey(stateUuid)) {
        if (!ObjectUtils.isEmpty(auditDetails.get(LAST_MODIFIED_TIME_KEY))) {
            Long lastModifiedTime = ((Number) auditDetails.get(LAST_MODIFIED_TIME_KEY)).longValue();
            Long slaDays = Math.round((stateUuidSlaMap.get(stateUuid) - (currentDate - lastModifiedTime)) / ((double) (24 * 60 * 60 * 1000)));
            log.info("SLA calculated based on state UUID {}: {} days remaining", stateUuid, slaDays);
            return slaDays;
        }
    }

    // SLA based on businessService
    if (businessService != null && businessServiceSlaMap.containsKey(businessService)) {
        if (!ObjectUtils.isEmpty(auditDetails.get(CREATED_TIME_KEY))) {
            Long createdTime = ((Number) auditDetails.get(CREATED_TIME_KEY)).longValue();
            Long businessServiceSLA = businessServiceSlaMap.get(businessService);
            Long slaDays = Math.round((businessServiceSLA - (currentDate - createdTime)) / ((double) (24 * 60 * 60 * 1000)));
            log.info("SLA calculated based on business service {}: {} days remaining", businessService, slaDays);
            return slaDays;
        }
    }

    log.warn("SLA could not be calculated for data: {}", data);
    return null;
}


    private List<HashMap<String,Object>> transformStatusMap(InboxRequest request,HashMap<String, Object> statusCountMap) {

        if(CollectionUtils.isEmpty(statusCountMap))
            return null;

        List<BusinessService> businessServices = workflowService.getBusinessServices(request);

        Map<String,String> statusIdToBusinessServiceMap = workflowService.getStatusIdToBusinessServiceMap(businessServices);
        Map<String, String> statusIdToApplicationStatusMap = workflowService.getApplicationStatusIdToStatusMap(businessServices);
        Map<String, String> statusIdToApplicationStateMap = workflowService.getApplicationStatusIdToStateMap(businessServices);

        List<HashMap<String,Object>> statusCountMapTransformed = new ArrayList<>();

        for(Map.Entry<String, Object> entry : statusCountMap.entrySet()){
            String statusId = entry.getKey();
            Integer count = (Integer) entry.getValue();
            HashMap<String, Object> map = new HashMap<>();
            map.put(COUNT_CONSTANT, count);
            map.put(APPLICATION_STATUS_KEY,statusIdToApplicationStatusMap.get(statusId));
            map.put(BUSINESSSERVICE_KEY,statusIdToBusinessServiceMap.get(statusId));
            map.put(STATUSID_KEY, statusId);
            map.put(STATE,statusIdToApplicationStateMap.get(statusId));
            statusCountMapTransformed.add(map);
        }
        return statusCountMapTransformed;
    }

    private HashMap<String, Object> parseStatusCountMapFromAggregationResponse(Map<String, Object> response, Set<String> actionableStatuses) {
        List<HashMap<String, Object>> statusCountResponse = new ArrayList<>();
        if(!CollectionUtils.isEmpty((Map<String, Object>) response.get(AGGREGATIONS_KEY))){
            List<Map<String, Object>> statusCountBuckets = JsonPath.read(response, STATUS_COUNT_AGGREGATIONS_BUCKETS_PATH);
            HashMap<String, Object> statusCountMap = new HashMap<>();
            statusCountBuckets.forEach(bucket -> {
                if(actionableStatuses.contains(bucket.get(KEY)))
                    statusCountMap.put((String)bucket.get(KEY), bucket.get(DOC_COUNT_KEY));
            });
            statusCountResponse.add(statusCountMap);
        }
        if(CollectionUtils.isEmpty(statusCountResponse))
            return null;

        return statusCountResponse.get(0);
    }

 private List<Inbox> parseInboxItemsFromSearchResponse(Object result, List<BusinessService> businessServices) {
    if (result == null) return new ArrayList<>();

    Map<String, Object> hitsMap = (Map<String, Object>) ((Map<String, Object>) result).get(HITS);
    if (hitsMap == null || CollectionUtils.isEmpty((List<?>) hitsMap.get(HITS))) {
        return new ArrayList<>();
    }

    List<Map<String, Object>> nestedHits = (List<Map<String, Object>>) hitsMap.get(HITS);

    // Use LinkedHashMap to preserve order
    Map<String, Inbox> propertyIdToInbox = new LinkedHashMap<>();

    for (Map<String, Object> hit : nestedHits) {
        Map<String, Object> sourceMap = (Map<String, Object>) hit.get(SOURCE_KEY);
        Map<String, Object> businessObject = (Map<String, Object>) sourceMap.get(DATA_KEY);
        if (businessObject == null) continue;

        // Safely fetch propertyId, fallback to some unique key if null
        String propertyId = (String) businessObject.get("propertyId");
        if (propertyId == null || propertyId.isEmpty()) {
            propertyId = UUID.randomUUID().toString(); // ensures unique key
        }

        // Only keep first/latest per propertyId
        if (!propertyIdToInbox.containsKey(propertyId)) {
            Inbox inbox = new Inbox();
            inbox.setBusinessObject(businessObject);

            // Enrich process instance
            if (businessObject.containsKey(CURRENT_PROCESS_INSTANCE_CONSTANT)) {
                ProcessInstance pi = mapper.convertValue(businessObject.get(CURRENT_PROCESS_INSTANCE_CONSTANT), ProcessInstance.class);
                inbox.setProcessInstance(pi);
                businessObject.remove(CURRENT_PROCESS_INSTANCE_CONSTANT);
            }

            propertyIdToInbox.put(propertyId, inbox);
        }
    }

    return new ArrayList<>(propertyIdToInbox.values());
}



    public Integer getApplicationsNearingSlaCount(InboxRequest inboxRequest, String indexName) {
        List<BusinessService> businessServicesObjs = workflowService.getBusinessServices(inboxRequest);
        Map<String, Long> businessServiceSlaMap = new HashMap<>();
        Map<String, HashSet<String>> businessServiceVsStateUuids = new HashMap<>();
        businessServicesObjs.forEach(businessService -> {
            List<String> listOfUuids = new ArrayList<>();
            businessService.getStates().forEach(state -> {
                listOfUuids.add(state.getUuid());
            });
            businessServiceVsStateUuids.put(businessService.getBusinessService(), new HashSet<>(listOfUuids));
            businessServiceSlaMap.put(businessService.getBusinessService(),businessService.getBusinessServiceSla());
        });

        List<String> uuidsInSearchCriteria = inboxRequest.getInbox().getProcessSearchCriteria().getStatus();

        Map<String, List<String>> businessServiceVsUuidsBasedOnSearchCriteria = new HashMap<>();

        // If status uuids are being passed in process search criteria, segregating them based on their business service
        if(!CollectionUtils.isEmpty(uuidsInSearchCriteria)) {
            uuidsInSearchCriteria.forEach(uuid -> {
                businessServiceVsStateUuids.keySet().forEach(businessService -> {
                    HashSet<String> setOfUuids = businessServiceVsStateUuids.get(businessService);
                    if (setOfUuids.contains(uuid)) {
                        if (businessServiceVsUuidsBasedOnSearchCriteria.containsKey(businessService)) {
                            businessServiceVsUuidsBasedOnSearchCriteria.get(businessService).add(uuid);
                        } else {
                            businessServiceVsUuidsBasedOnSearchCriteria.put(businessService, new ArrayList<>(Collections.singletonList(uuid)));
                        }
                    }
                });

            });
        }else{
            businessServiceVsStateUuids.keySet().forEach(businessService -> {
                HashSet<String> setOfUuids = businessServiceVsStateUuids.get(businessService);
                businessServiceVsUuidsBasedOnSearchCriteria.put(businessService, new ArrayList<>(setOfUuids));
            });
        }



        List<String> businessServices = new ArrayList<>(businessServiceVsUuidsBasedOnSearchCriteria.keySet());
        Integer totalCount = 0;
        // Fetch slot percentage only once here !!!!!!!!!!


        for(int i = 0; i < businessServices.size(); i++){
            String businessService = businessServices.get(i);
            Long businessServiceSla = businessServiceSlaMap.get(businessService);
            inboxRequest.getInbox().getProcessSearchCriteria().setStatus(businessServiceVsUuidsBasedOnSearchCriteria.get(businessService));
            Map<String, Object> finalQueryBody = queryBuilder.getNearingSlaCountQuery(inboxRequest, businessServiceSla);
            StringBuilder uri = getURI(indexName, COUNT_PATH);
            Map<String, Object> response = (Map<String, Object>) serviceRequestRepository.fetchESResult(uri, finalQueryBody);
            Integer currentCount = 0;
            if(response.containsKey(COUNT_CONSTANT)){
                currentCount = (Integer) response.get(COUNT_CONSTANT);
            }else{
                throw new CustomException("INBOX_COUNT_ERR", "Error occurred while executing ES count query");
            }
            totalCount += currentCount;
        }

        return totalCount;

    }


    private StringBuilder getURI(String indexName, String endpoint){
        StringBuilder uri = new StringBuilder(config.getIndexServiceHost());
        uri.append(indexName);
        uri.append(endpoint);
        return uri;
    }

    public SearchResponse getSpecificFieldsFromESIndex(SearchRequest searchRequest) {
        String tenantId = searchRequest.getIndexSearchCriteria().getTenantId();
        String moduleName = searchRequest.getIndexSearchCriteria().getModuleName();
        Map<String, Object> moduleSearchCriteria = searchRequest.getIndexSearchCriteria().getModuleSearchCriteria();

        validator.validateSearchCriteria(tenantId, moduleName, moduleSearchCriteria);
        InboxQueryConfiguration inboxQueryConfiguration = mdmsUtil.getConfigFromMDMS(tenantId, moduleName);
        hashParamsWhereverRequiredBasedOnConfiguration(moduleSearchCriteria, inboxQueryConfiguration);
        List<Data> data = getDataFromSimpleSearch(searchRequest, inboxQueryConfiguration.getIndex());
        SearchResponse searchResponse = SearchResponse.builder().data(data).build();
        return searchResponse;
    }

    private List<Data> getDataFromSimpleSearch(SearchRequest searchRequest, String index) {
        Map<String, Object> finalQueryBody = queryBuilder.getESQueryForSimpleSearch(searchRequest, Boolean.TRUE);
        try {
            String q = mapper.writeValueAsString(finalQueryBody);
            log.info("Query: "+q);
        }
        catch (Exception e){
            e.printStackTrace();
        }
        StringBuilder uri = getURI(index, SEARCH_PATH);
        Object result = serviceRequestRepository.fetchESResult(uri, finalQueryBody);
        List<Data> dataList = parseSearchResponseForSimpleSearch(result);
        return dataList;
    }

    private List<Data> parseSearchResponseForSimpleSearch(Object result) {
        Map<String, Object> hits = (Map<String, Object>)((Map<String, Object>) result).get(HITS);
        List<Map<String, Object>> nestedHits = (List<Map<String, Object>>) hits.get(HITS);
        if(CollectionUtils.isEmpty(nestedHits)){
            return new ArrayList<>();
        }

        List<Data> dataList = new ArrayList<>();
        nestedHits.forEach(hit -> {
            Data data = new Data();
            Map<String, Object> sourceObject = (Map<String, Object>) hit.get(SOURCE_KEY);
            Map<String, Object> dataObject = (Map<String, Object>)sourceObject.get(DATA_KEY);
            List<Field> fields = getFieldsFromDataObject(dataObject);
            data.setFields(fields);
            dataList.add(data);
        });

        return dataList;
    }

    private List<Field> getFieldsFromDataObject(Map<String, Object> dataObject) {
        List<Field> listOfFields = new ArrayList<>();
        try {
            Map<String, Object> flattenedDataObject = JsonFlattener.flattenAsMap(mapper.writeValueAsString(dataObject));
            flattenedDataObject.keySet().forEach(key -> {
                Field field = new Field();
                field.setKey(key);
                field.setValue(flattenedDataObject.get(key));
                listOfFields.add(field);
            });
        }catch (JsonProcessingException ex){
            throw new CustomException("EG_INBOX_GET_FIELDS_ERR", "Error while processing JSON.");
        }
        return listOfFields;
    }
}
