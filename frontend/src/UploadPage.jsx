import { useState } from "react";
import api from "./api";

function UploadPage({ onSuccess }) {
  const [file, setFile] = useState(null);
  const [error, setError] = useState("");
  const [isUploading, setIsUploading] = useState(false);

  const handleUpload = async () => {
    if (!file) {
      setError("Choose a file first.");
      return;
    }

    setError("");
    setIsUploading(true);

    try {
      const formData = new FormData();
      formData.append("file", file);

      const uploadRes = await api.post("/api/upload", formData);

      const fileId = uploadRes.data.file_id;

      const detectRes = await api.get(`/api/detect/${fileId}`);

      onSuccess({
        file_id: fileId,
        detected_columns: detectRes.data.columns || [],
      });
    } catch (err) {
      const detail = err?.response?.data?.detail;
      setError(
        typeof detail === "string" ? detail : "Upload failed. Check the file and try again."
      );
    } finally {
      setIsUploading(false);
    }
  };

  return (
    <div>
      <h2 className="text-xl font-semibold text-slate-900">Upload</h2>

      <div className="mt-4">
        <input
          type="file"
          accept=".csv,.xls,.xlsx"
          onChange={(event) => setFile(event.target.files?.[0] || null)}
        />
      </div>

      {file && (
        <p className="mt-2 text-sm text-slate-600">
          Selected: {file.name}
        </p>
      )}

      {error && (
        <div className="mt-4 rounded border border-red-300 bg-red-50 p-3 text-sm text-red-700">
          {error}
        </div>
      )}

      <button
        type="button"
        onClick={handleUpload}
        disabled={isUploading}
        className="mt-6 rounded bg-slate-900 px-4 py-2 text-white disabled:opacity-50"
      >
        {isUploading ? "Uploading..." : "Upload"}
      </button>
    </div>
  );
}

export default UploadPage;