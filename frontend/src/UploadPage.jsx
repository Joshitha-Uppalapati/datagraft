import { useRef, useState } from "react";
import api from "./api";

const sampleCsv = `fname,email address,ph no,signup date,amt
Josh,josh@example.com,1234567890,2024-01-15,$12.50
Jane,jane@example.com,9876543210,2024-02-20,$22.75
Bad,bad-email,123,not-a-date,$abc
Josh,josh@example.com,1234567890,2024-01-15,$12.50
`;

function UploadPage({ onSuccess }) {
  const inputRef = useRef(null);
  const [file, setFile] = useState(null);
  const [status, setStatus] = useState("");
  const [error, setError] = useState("");
  const [isUploading, setIsUploading] = useState(false);

  const uploadFile = async (selectedFile) => {
    if (!selectedFile || isUploading) return;

    const formData = new FormData();
    formData.append("file", selectedFile);

    setIsUploading(true);
    setError("");
    setStatus("Uploading file...");

    try {
      const uploadRes = await api.post("/api/upload", formData);

      setStatus("Detecting columns...");

      const detectRes = await api.get(
        `/api/detect/${uploadRes.data.file_id}`
      );

      onSuccess({
        file_id: uploadRes.data.file_id,
        filename: uploadRes.data.filename,
        row_count: uploadRes.data.row_count,
        col_count: uploadRes.data.col_count,
        detected_columns: detectRes.data.columns,
      });
    } catch (err) {
      setError(err?.response?.data?.detail || "Upload failed.");
      setStatus("");
    } finally {
      setIsUploading(false);
    }
  };

  const handleFileChange = (event) => {
    const selectedFile = event.target.files?.[0];

    setFile(selectedFile || null);
    setError("");
    setStatus("");
  };

  const handleUpload = () => {
    uploadFile(file);
  };

  const handleSampleUpload = () => {
    const sampleFile = new File([sampleCsv], "messy_columns_sample.csv", {
      type: "text/csv",
    });

    setFile(sampleFile);
    uploadFile(sampleFile);
  };

  return (
    <div>
      <div className="mb-6">
        <h2 className="text-xl font-semibold">Upload</h2>
        <p className="mt-1 text-sm text-slate-500">
          Start with a CSV or try the built-in messy sample.
        </p>
      </div>

      <div className="rounded-xl border border-dashed border-slate-300 bg-slate-50 p-8">
        <input
          ref={inputRef}
          type="file"
          accept=".csv,.xlsx,.xls"
          onChange={handleFileChange}
          className="hidden"
        />

        <div className="flex flex-col gap-4 sm:flex-row sm:items-center">
          <button
            type="button"
            onClick={() => inputRef.current?.click()}
            className="rounded bg-white px-4 py-2 text-sm font-medium text-slate-800 ring-1 ring-slate-300 hover:bg-slate-100"
          >
            Choose File
          </button>

          <button
            type="button"
            onClick={handleUpload}
            disabled={!file || isUploading}
            className="rounded bg-slate-900 px-4 py-2 text-sm font-medium text-white hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-50"
          >
            {isUploading ? "Processing..." : "Upload File"}
          </button>

          <button
            type="button"
            onClick={handleSampleUpload}
            disabled={isUploading}
            className="rounded border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-white disabled:cursor-not-allowed disabled:opacity-50"
          >
            Try Sample Data
          </button>
        </div>

        {file && (
          <p className="mt-4 text-sm text-slate-600">
            Selected: <span className="font-medium">{file.name}</span>
          </p>
        )}

        {status && (
          <p className="mt-4 text-sm font-medium text-slate-700">
            {status}
          </p>
        )}

        {error && (
          <div className="mt-4 rounded border border-red-300 bg-red-50 p-3 text-sm text-red-700">
            {error}
          </div>
        )}
      </div>
    </div>
  );
}

export default UploadPage;