package AEMVisionAI.core.workflow;

import com.adobe.granite.workflow.WorkflowSession;
import com.adobe.granite.workflow.exec.WorkItem;
import com.adobe.granite.workflow.exec.WorkflowProcess;
import com.adobe.granite.workflow.metadata.MetaDataMap;
import com.day.cq.dam.api.Asset;
import com.day.cq.dam.api.Rendition;
import com.day.cq.tagging.InvalidTagFormatException;
import com.day.cq.tagging.Tag;
import com.day.cq.tagging.TagManager;
import com.google.cloud.vision.v1.*;
import com.google.protobuf.ByteString;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.tika.io.IOUtils;
import org.osgi.service.component.annotations.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

@Component(service = WorkflowProcess.class, property = {"process.label=AI Tag Generation"}, enabled = true, immediate = true)
public class VisionAITagsWorkflowStep implements WorkflowProcess {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    @Override
    public void execute(WorkItem workItem, WorkflowSession workflowSession, MetaDataMap metaDataMap) {
        ResourceResolver resourceResolver = workflowSession.adaptTo(ResourceResolver.class);
        try {
            String imagePath = workItem.getWorkflowData().getPayload().toString();
            try (ImageAnnotatorClient vision = ImageAnnotatorClient.create()) {
                List<String> description = new ArrayList<>();

                // Resource to Annotate
                Resource resource = resourceResolver.getResource(imagePath);
                InputStream is = imagePath.endsWith("/renditions/original") ?
                        resource.adaptTo(Rendition.class).getStream() :
                        resource.adaptTo(Asset.class).getRendition("original").getStream();
                Resource metaDataNode = imagePath.endsWith("/renditions/original") ?
                        resource.getParent().getParent().getChild("metadata") :
                        resource.getChild("jcr:content/metadata");
                byte[] data = IOUtils.toByteArray(is);
                ByteString imgBytes = ByteString.copyFrom(data);

                // Builds the image annotation request
                List<AnnotateImageRequest> requests = new ArrayList<>();
                Image img = Image.newBuilder().setContent(imgBytes).build();
                Feature feat = Feature.newBuilder().setType(Feature.Type.LABEL_DETECTION).build();
                AnnotateImageRequest request = AnnotateImageRequest.newBuilder()
                        .addFeatures(feat)
                        .setImage(img)
                        .build();
                requests.add(request);


                // Performs label detection on the image file
                BatchAnnotateImagesResponse response = vision.batchAnnotateImages(requests);
                List<AnnotateImageResponse> responses = response.getResponsesList();
                for (AnnotateImageResponse res : responses) {
                    if (res.hasError()) {
                        return;
                    }

                    for (EntityAnnotation annotation : res.getLabelAnnotationsList()) {
                        annotation.getAllFields().forEach((k, v) -> {
                            if (k.getJsonName().equalsIgnoreCase("description")) {
                                description.add(v.toString());
                            }
                        });
                    }
                }
                //Generate Tags Locally and Tag Asset
                TagManager tagManager = resourceResolver.adaptTo(TagManager.class);
                Tag[] tags = generateTagsFromCloudAI(description, tagManager);
                tagManager.setTags(metaDataNode, tags, true);

            }
        } catch (Exception e) {
            log.error("Exception in VisionAIWorkflowStep {}", e.getMessage(), e);
        }
    }

    private Tag[] generateTagsFromCloudAI(List<String> description, TagManager tagManager) throws InvalidTagFormatException {
        List<Tag> tagList = new ArrayList<>();
        for (String tagId : description) {
            Tag tag = tagManager.createTag("GoogleCloudVisionAI/" + tagId, tagId, "AI generated tag", true);
            tagList.add(tag);
        }
        return tagList.toArray(new Tag[0]);
    }
}