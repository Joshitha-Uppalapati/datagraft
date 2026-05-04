import { useState } from "react";
import axios from "axios";

function UploadPage({ onUploadSuccess }) {
  const [file, setFile] = useState(null);

  const handleUpload = async () => {
    if (!file) return;

    const formData = new FormData();
    formData.append("file", file);

    const res = await axios.post(
      "http://localhost:8000/api/upload",
      formData
    );

    onUploadSuccess(res.data);
  };

  return (
    <div>
      <h2>Upload</h2>

      <input
        type="file"
        onChange={(e) => setFile(e.target.files[0])}
      />

      <button onClick={handleUpload}>Upload</button>
    </div>
  );
}

export default UploadPage;