
import { useState } from "react";
import axios from "axios";

const PRESETS = {
  contact: [
    { name: "first_name", required: true },
    { name: "last_name", required: false },
    { name: "email", required: true },
    { name: "phone", required: false },
  ],
  transaction: [
    { name: "date", required: true },
    { name: "amount", required: true },
    { name: "description", required: false },
  ],
};

function MappingPage({ fileId, detectedColumns = [], onComplete }) {
  const [presetKey, setPresetKey] = useState("contact");
  const [mappings, setMappings] = useState([]);
  const [localMappings, setLocalMappings] = useState([]);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [error, setError] = useState(null);

  const selectedSchema = PRESETS[presetKey];

  const handleRunMapping = async () => {
    setError(null);
    setIsSubmitting(true);

    try {
      const res = await axios.post(
        `http://localhost:8000/api/map/${fileId}`,
        {
          target_schema: selectedSchema.map((f) => ({
            name: f.name,
            type: "string",
            required: f.required,
            variants: [],
          })),
        }
      );

      setMappings(res.data.mappings);

      setLocalMappings(
        res.data.mappings.map((m) => ({
          original: m.original,
          canonical: m.suggested_canonical || "",
        }))
      );
    } catch (err) {
      setError(err?.response?.data?.detail || "Mapping failed");
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleConfirm = async () => {
    setError(null);

    const missingRequired = selectedSchema
      .filter((f) => f.required)
      .some(
        (f) => !localMappings.find((m) => m.canonical === f.name)
      );

    if (missingRequired) {
      alert("Map all required fields before confirming.");
      return;
    }

    setIsSubmitting(true);

    try {
      await axios.post(
        `http://localhost:8000/api/map/${fileId}/confirm`,
        { auto_confirm: true }
      );

      onComplete();
    } catch (err) {
      setError(err?.response?.data?.detail || "Confirm failed");
    } finally {
      setIsSubmitting(false);
    }
  };

  const getConfidenceClass = (confidence) => {
    if (confidence >= 0.8) return "bg-green-100 text-green-700";
    if (confidence >= 0.6) return "bg-yellow-100 text-yellow-700";
    return "bg-red-100 text-red-700";
  };

  const hasMappings = localMappings.some((m) => m.canonical);

  return (
    <div className="mt-6">
      <h2 className="text-xl font-semibold mb-2">Mapping</h2>
      <p className="text-sm text-gray-500 mb-4">fileId: {fileId}</p>

      <div className="mb-4">
        <select
          value={presetKey}
          onChange={(e) => setPresetKey(e.target.value)}
          className="border rounded px-3 py-2"
        >
          <option value="contact">Contact List</option>
          <option value="transaction">Transaction Log</option>
        </select>

        <button
          onClick={handleRunMapping}
          disabled={isSubmitting}
          className="ml-3 rounded bg-slate-900 px-4 py-2 text-white disabled:opacity-50"
        >
          {isSubmitting ? "Processing..." : "Run Mapping"}
        </button>
      </div>

      {error && (
        <div className="mb-4 p-3 bg-red-100 text-red-700 rounded">
          {error}
        </div>
      )}

      {mappings.length > 0 && (
        <table className="w-full text-left border-collapse">
          <thead className="bg-gray-50 border-b">
            <tr>
              <th className="px-4 py-2">Original</th>
              <th className="px-4 py-2">Type</th>
              <th className="px-4 py-2">Suggested Match</th>
              <th className="px-4 py-2">Confidence</th>
            </tr>
          </thead>

          <tbody>
            {mappings.map((mapping) => (
              <tr key={mapping.original} className="border-b">
                <td className="px-4 py-2">{mapping.original}</td>
                <td className="px-4 py-2">{mapping.inferred_type}</td>

                <td className="px-4 py-2">
                  <select
                    value={
                      localMappings.find(
                        (m) => m.original === mapping.original
                      )?.canonical || ""
                    }
                    onChange={(e) => {
                      setLocalMappings((prev) =>
                        prev.map((m) =>
                          m.original === mapping.original
                            ? { ...m, canonical: e.target.value }
                            : m
                        )
                      );
                    }}
                    className="border rounded px-2 py-1 w-full"
                  >
                    <option value="">(skip)</option>
                    {selectedSchema.map((field) => (
                      <option key={field.name} value={field.name}>
                        {field.name}
                      </option>
                    ))}
                  </select>
                </td>

                <td className="px-4 py-2">
                  <span
                    className={`px-2 py-1 rounded text-sm ${getConfidenceClass(
                      mapping.confidence
                    )}`}
                  >
                    {(mapping.confidence * 100).toFixed(0)}%
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {mappings.length > 0 && (
        <button
          onClick={handleConfirm}
          disabled={isSubmitting || !hasMappings}
          className="mt-6 rounded bg-slate-900 px-4 py-2 text-white disabled:opacity-50 disabled:cursor-not-allowed"
        >
          {isSubmitting ? "Processing..." : "Confirm Mappings"}
        </button>
      )}
    </div>
  );
}

export default MappingPage;