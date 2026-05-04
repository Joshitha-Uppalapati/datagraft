import { useState } from "react";
import UploadPage from "./UploadPage";
import MappingPage from "./MappingPage";
import ValidationPage from "./ValidationPage";

function App() {
  const [step, setStep] = useState("upload");
  const [fileId, setFileId] = useState(null);

  const handleUploadSuccess = (data) => {
    setFileId(data.file_id);
    setStep("mapping");
  };

  const handleMappingConfirmed = () => {
    setStep("validation");
  };

  const handleValidationDone = () => {
    setStep("export");
  };

  return (
    <div style={{ padding: 40 }}>
      <h1>DataGraft</h1>

      {step === "upload" && (
        <UploadPage onUploadSuccess={handleUploadSuccess} />
      )}

      {step === "mapping" && fileId && (
        <MappingPage
          fileId={fileId}
          onMappingConfirmed={handleMappingConfirmed}
        />
      )}

      {step === "validation" && fileId && (
        <ValidationPage
          fileId={fileId}
          onValidationDone={handleValidationDone}
        />
      )}

      {step === "export" && fileId && (
        <div>
          <h2>Export Step</h2>
          <p>fileId: {fileId}</p>
          <a
            href={`http://localhost:8000/api/export/${fileId}`}
            target="_blank"
            rel="noreferrer"
          >
            Download Clean CSV
          </a>
        </div>
      )}
    </div>
  );
}

export default App;