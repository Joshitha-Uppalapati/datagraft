import { useState } from "react";
import axios from "axios";

function App() {
  const [file, setFile] = useState(null);
  const [uploadResult, setUploadResult] = useState(null);
  const [detectResult, setDetectResult] = useState(null);
  const [mappingResult, setMappingResult] = useState(null);
  const [confirmResult, setConfirmResult] = useState(null);
  const [loading, setLoading] = useState(false);

  const handleUpload = async () => {
    if (!file) return;

    const formData = new FormData();
    formData.append("file", file);

    try {
      setLoading(true);
      const res = await axios.post(
        "http://localhost:8000/api/upload",
        formData
      );

      setUploadResult(res.data);
      setDetectResult(null);
      setMappingResult(null);
      setConfirmResult(null);
    } catch (err) {
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  const handleDetect = async () => {
    try {
      setLoading(true);
      const res = await axios.get(
        `http://localhost:8000/api/detect/${uploadResult.file_id}`
      );

      setDetectResult(res.data);
    } catch (err) {
      setDetectResult({
        error: err?.response?.data?.detail || "detect failed",
      });
    } finally {
      setLoading(false);
    }
  };

  const handleMap = async () => {
    try {
      setLoading(true);

      const payload = {
        target_schema: [
          { name: "first_name", type: "string", required: true, variants: [] },
          { name: "email", type: "email", required: true, variants: [] },
          { name: "phone", type: "phone", required: true, variants: [] },
          { name: "signup_date", type: "date", required: true, variants: [] },
          { name: "amount", type: "float", required: true, variants: [] },
        ],
      };

      const res = await axios.post(
        `http://localhost:8000/api/map/${uploadResult.file_id}`,
        payload
      );

      setMappingResult(res.data);
    } catch (err) {
      setMappingResult({
        error: err?.response?.data?.detail || "mapping failed",
      });
    } finally {
      setLoading(false);
    }
  };

  const handleConfirm = async () => {
    try {
      setLoading(true);

      const res = await axios.post(
        `http://localhost:8000/api/map/${uploadResult.file_id}/confirm`,
        {
          auto_confirm: true,
        }
      );

      setConfirmResult(res.data);
    } catch (err) {
      setConfirmResult({
        error: err?.response?.data?.detail || "confirm failed",
      });
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={{ padding: 40 }}>
      <h1>DataGraft</h1>

      <input type="file" onChange={(e) => setFile(e.target.files[0])} />
      <button onClick={handleUpload} disabled={loading}>
        Upload
      </button>

      {uploadResult && (
        <>
          <pre>{JSON.stringify(uploadResult, null, 2)}</pre>
          <button onClick={handleDetect} disabled={loading}>
            Run Detection
          </button>
        </>
      )}

      {detectResult && (
        <>
          <pre>{JSON.stringify(detectResult, null, 2)}</pre>
          <button onClick={handleMap} disabled={loading}>
            Run Mapping
          </button>
        </>
      )}

      {mappingResult && (
        <>
          <pre>{JSON.stringify(mappingResult, null, 2)}</pre>
          <button onClick={handleConfirm} disabled={loading}>
            Confirm Mapping
          </button>
        </>
      )}

      {confirmResult && (
        <pre>{JSON.stringify(confirmResult, null, 2)}</pre>
      )}
    </div>
  );
}

export default App;