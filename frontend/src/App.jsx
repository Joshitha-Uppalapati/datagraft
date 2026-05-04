import { useState } from "react";
import UploadPage from "./UploadPage";
import MappingPage from "./MappingPage";

const steps = ["upload", "mapping", "validation", "export"];

function App() {
  const [step, setStep] = useState("upload");
  const [fileId, setFileId] = useState(null);
  const [detectedColumns, setDetectedColumns] = useState([]);
  const [validationSummary, setValidationSummary] = useState(null);

  const handleUploadSuccess = (data) => {
    setFileId(data.file_id);
    setDetectedColumns(data.detected_columns || data.columns || []);
    setValidationSummary(null);
    setStep("mapping");
  };

  const handleMappingComplete = () => {
    setStep("validation");
  };

  return (
    <main className="min-h-screen bg-slate-50 px-6 py-8 text-slate-900">
      <div className="mx-auto max-w-5xl">
        <h1 className="text-3xl font-semibold">DataGraft</h1>

        <div className="mt-6 flex gap-3">
          {steps.map((item, index) => (
            <div
              key={item}
              className={`rounded-full px-4 py-2 text-sm ${
                step === item
                  ? "bg-slate-900 text-white"
                  : "bg-white text-slate-600 border border-slate-200"
              }`}
            >
              {index + 1}. {item}
            </div>
          ))}
        </div>

        <section className="mt-8 rounded-xl border border-slate-200 bg-white p-6 shadow-sm">
          {step === "upload" && (
            <UploadPage onSuccess={handleUploadSuccess} />
          )}

          {step === "mapping" && (
            <MappingPage
              fileId={fileId}
              detectedColumns={detectedColumns}
              onComplete={handleMappingComplete}
            />
          )}

          {step === "validation" && (
            <div>
              <h2 className="text-xl font-semibold">Validation</h2>
              <p className="mt-2 text-slate-600">
                Validation page placeholder.
              </p>
              <p className="mt-2 text-sm text-slate-500">fileId: {fileId}</p>
            </div>
          )}

          {step === "export" && (
            <div>
              <h2 className="text-xl font-semibold">Export</h2>
              <p className="mt-2 text-slate-600">Export page placeholder.</p>
              <p className="mt-2 text-sm text-slate-500">fileId: {fileId}</p>

              {validationSummary && (
                <pre className="mt-4 rounded bg-slate-100 p-4 text-sm">
                  {JSON.stringify(validationSummary, null, 2)}
                </pre>
              )}
            </div>
          )}
        </section>
      </div>
    </main>
  );
}

export default App;