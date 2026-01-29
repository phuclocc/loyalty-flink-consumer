package vn.ghtk.loyalty.flink.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * Event metadata
 */
public class EventMetadata implements Serializable {
    
    @JsonProperty("source")
    private String source;
    
    @JsonProperty("version")
    private String version;
    
    @JsonProperty("requestId")
    private String requestId;
    
    @JsonProperty("clientIp")
    private String clientIp;
    
    @JsonProperty("userAgent")
    private String userAgent;

    public EventMetadata() {
    }

    public EventMetadata(String source, String version, String requestId, 
                        String clientIp, String userAgent) {
        this.source = source;
        this.version = version;
        this.requestId = requestId;
        this.clientIp = clientIp;
        this.userAgent = userAgent;
    }

    // Getters and Setters
    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public String getClientIp() {
        return clientIp;
    }

    public void setClientIp(String clientIp) {
        this.clientIp = clientIp;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    @Override
    public String toString() {
        return "EventMetadata{" +
                "source='" + source + '\'' +
                ", version='" + version + '\'' +
                ", requestId='" + requestId + '\'' +
                ", clientIp='" + clientIp + '\'' +
                ", userAgent='" + userAgent + '\'' +
                '}';
    }
}
