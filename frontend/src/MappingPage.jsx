import { useState } from "react";
import axios from "axios";

const presetSchemas = {
  contact: {
    label: "Contact List",
    fields: [
      { name: "first_name", type: "string", required: true, variants: [] },
      { name: "last_name", type: "string", required: false, variants: [] },
      { name: "email", type: "email", required: true, variants: [] },
      { name: "phone", type: "phone", required: false, variants: [] },
    ],
  },
  transaction: {
    label: "Transaction Log",
    fields: [
      { name: "date", type: "date", required: true, variants: [] },
      { name: "amount", type: "float", required: true, variants: [] },
      { name: "description", type: "string", required: false, variants: [] },
    ],
  },
};

function confidenceClass(confidence) {
  if (confidence >= 0.8) {
    return "bg-green-100 text-green-800";
  }

  if (confidence >= 0.6) {
    return "bg-yellow-100 text-yellow-800";
  }

  return "bg-red-100 text-red-800";
}

function MappingPage({ fileId, detectedColumns, onComplete }) {
  const [schemaKey, setSchemaKey] = useState("contact");
  const [mappings, setMappings] = useState([]);
  const [error, setError] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);

  const confirmMappings = async () => {
    setError("");
    setIsSubmitting(true);

    try {
      const schema = presetSchemas[schemaKey].fields;

      const mapResponse = await axios.post(
        `http://localhost:8000/api/map/${fileId}`,
        { target_schema: schema }
      );

      setMappings(mapResponse.data.mappings || []);

      await axios.post(
        `http://localhost:8000/api/map/${fileId}/confirm`,
        { auto_confirm: true }
      );

      onComplete();
    } catch (err) {
      const detail = err?.response?.data?.detail;
      setError(
        typeof detail === "string" ? detail : JSON.stringify(detail || err.message)
      );
    } finally {
      setIsSubmitting(false);
    }
  };

  const rows = mappings.length
    ? mappings
    : detectedColumns.map((column) => ({
        original: column.original_name,
        suggested_canonical: "",
        confidence: 0,
      }));

  return (
    <div>
      <div className="flex items-center justify-between gap-4">
        <div>
          <h2 className="text-xl font-semibold">Mapping</h2>
          <p className="mt-1 text-sm text-slate-500">fileId: {fileId}</p>
        </div>

        <select
          value={schemaKey}
          onChange={(event) => setSchemaKey(event.target.value)}
          className="rounded border border-slate-300 bg-white px-3 py-2"
        >
          {Object.entries(presetSchemas).map(([key, schema]) => (
            <option key={key} value={key}>
              {schema.label}
            </option>
          ))}
        </select>
      </div>

      {error && (
        <div className="mt-4 rounded border border-red-300 bg-red-50 p-3 text-sm text-red-700">
          {error}
        </div>
      )}

      <table className="mt-6 w-full border-collapse text-left text-sm">
        <thead>
          <tr className="border-b bg-slate-100">
            <th className="p-3">Original Name</th>
            <th className="p-3">Detected Type</th>
            <th className="p-3">Suggested Match</th>
            <th className="p-3">Confidence</th>
          </tr>
        </thead>

        <tbody>
          {rows.map((row) => {
            const detected = detectedColumns.find(
              (column) => column.original_name === row.original
            );

            return (
              <tr key={row.original} className="border-b">
                <td className="p-3">{row.original}</td>
                <td className="p-3">{detected?.inferred_type || "-"}</td>
                <td className="p-3">{row.suggested_canonical || "-"}</td>
                <td className="p-3">
                  <span
                    className={`rounded-full px-2 py-1 text-xs font-medium ${confidenceClass(
                      row.confidence || 0
                    )}`}
                  >
                    {Math.round((row.confidence || 0) * 100)}%
                  </span>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>

      <button
        onClick={confirmMappings}
        disabled={isSubmitting}
        className="mt-6 rounded bg-slate-900 px-4 py-2 text-white disabled:opacity-50"
      >
        {isSubmitting ? "Confirming..." : "Confirm Mappings"}
      </button>
    </div>
  );
}

export default MappingPage;