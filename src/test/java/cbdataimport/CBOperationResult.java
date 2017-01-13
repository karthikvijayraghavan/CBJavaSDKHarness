package cbdataimport;

import com.couchbase.client.java.document.json.JsonObject;

public class CBOperationResult {

	private String id;
	private String record;
	private JsonObject result;
	private Throwable error;
	private CBOperationResultStatus status;

	public CBOperationResult(String id, String record,
			JsonObject result, Throwable error,
			CBOperationResultStatus status) {
		this.id = id;
		this.result = result;
		this.error = error;
		this.status = status;
		this.record = record;
	}

	public JsonObject getResult() {
		return result;
	}

	public void setResult(JsonObject result) {
		this.result = result;
	}

	public String getRecord() {
		return this.record;
	}

	public void setRecord(String record) {
		this.record = record;
	}

	public Throwable getError() {
		return error;
	}

	public void setError(Throwable error) {
		this.error = error;
	}

	public CBOperationResultStatus getStatus() {
		return status;
	}

	public void setStatus(CBOperationResultStatus status) {
		this.status = status;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
}
