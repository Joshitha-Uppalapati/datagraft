import { useState } from "react";
import UploadPage from "./UploadPage";
import MappingPage from "./MappingPage";

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

  const handleValidationComplete = () => {
    setStep("export");
  };

  return (
    <div style={{ padding: 40 }}>
      <h1>DataGraft</h1>

      {step === "upload" && (
        <UploadPage onUploadSuccess={handleUploadSuccess} />
      )}

      {step === "mapping" && (
        <MappingPage
        fileId={fileId}
        onMappingConfirmed={handleMappingConfirmed}
      />  
    )}

      {step === "validation" && (
        <div>
          <h2>Validation Step</h2>
          <p>fileId: {fileId}</p>
        </div>
      )}

      {step === "export" && (
        <div>
          <h2>Export Step</h2>
          <p>fileId: {fileId}</p>
        </div>
      )}
    </div>
  );
}

export default App;