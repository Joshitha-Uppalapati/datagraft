import { useState } from "react";
import api from "./api";

const PRESETS = {
  contact: [
    { name: "first_name", type: "string", required: true },
    { name: "last_name", type: "string", required: false },
    { name: "email", type: "email", required: true },
    { name: "phone", type: "phone", required: false },
  ],
  transaction: [
    { name: "date", type: "date", required: true },
    { name: "amount", type: "float", required: true },
    { name: "description", type: "string", required: false },
  ],
};

function MappingPage({ fileId, detectedColumns = [], onComplete }) {
  const [presetKey, setPresetKey] = useState("contact");
  const [mappings, setMappings] = useState([]);
  const [localMappings, setLocalMappings] = useState([]);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [error, setError] = useState(null);

  const selectedSchema = PRESETS[presetKey];

  const selectedCanonicals = localMappings
    .map((mapping) => mapping.canonical)
    .filter(Boolean);

  const hasMappings = selectedCanonicals.length > 0;

  const missingRequired = selectedSchema
    .filter((field) => field.required)
    .some(
      (field) =>
        !localMappings.some((mapping) => mapping.canonical === field.name)
    );

  const hasDuplicateTargets =
    selectedCanonicals.length !== new Set(selectedCanonicals).size;

  const confirmDisabled =
    isSubmitting || !hasMappings || missingRequired || hasDuplicateTargets;

  const formatError = (err, fallback) => {
    const detail = err?.response?.data?.detail;

    if (typeof detail === "string") {
      return detail;
    }

    if (detail) {
      return JSON.stringify(detail);
    }

    return fallback;
  };

  const handlePresetChange = (event) => {
    setPresetKey(event.target.value);
    setMappings([]);
    setLocalMappings([]);
    setError(null);
  };

  const handleRunMapping = async () => {
    setError(null);
    setIsSubmitting(true);

    try {
      const res = await api.post(
        `/api/map/${fileId}`,
        {
          target_schema: selectedSchema.map((field) => ({
            name: field.name,
            type: field.type,
            required: field.required,
            variants: [],
          })),
        }
      );

      const apiMappings = res.data.mappings || [];

      setMappings(apiMappings);
      setLocalMappings(
        apiMappings.map((mapping) => ({
          original: mapping.original,
          canonical: mapping.suggested_canonical || "",
        }))
      );
    } catch (err) {
      setError(formatError(err, "Mapping failed"));
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleConfirm = async () => {
    setError(null);

    if (missingRequired) {
      setError("Map all required fields before confirming.");
      return;
    }

    if (hasDuplicateTargets) {
      setError("Each target field can only be mapped once.");
      return;
    }

    const confirmedMappings = localMappings
      .filter((mapping) => mapping.canonical)
      .map((mapping) => ({
        original: mapping.original,
        canonical: mapping.canonical,
      }));

    setIsSubmitting(true);

    try {
      await api.post(
        `/api/map/${fileId}/confirm`,
        {
          confirmed_mappings: confirmedMappings,
        }
      );

      onComplete();
    } catch (err) {
      setError(formatError(err, "Confirm failed"));
    } finally {
      setIsSubmitting(false);
    }
  };

  const getConfidenceClass = (confidence) => {
    if (confidence >= 0.8) return "bg-green-100 text-green-700";
    if (confidence >= 0.6) return "bg-yellow-100 text-yellow-700";
    return "bg-red-100 text-red-700";
  };

  const getDetectedType = (originalName) => {
    return (
      detectedColumns.find((column) => column.original_name === originalName)
        ?.inferred_type || "-"
    );
  };

  return (
    <div className="mt-6">
      <h2 className="mb-2 text-xl font-semibold">Mapping</h2>

      <div className="mb-4 flex items-center gap-3">
        <select
          value={presetKey}
          onChange={handlePresetChange}
          disabled={isSubmitting}
          className="rounded border px-3 py-2 disabled:opacity-50"
        >
          <option value="contact">Contact List</option>
          <option value="transaction">Transaction Log</option>
        </select>

        <button
          onClick={handleRunMapping}
          disabled={isSubmitting}
          className="rounded bg-slate-900 px-4 py-2 text-white disabled:cursor-not-allowed disabled:opacity-50"
        >
          {isSubmitting ? "Processing..." : "Run Mapping"}
        </button>
      </div>

      {error && (
        <div className="mb-4 rounded bg-red-100 p-3 text-red-700">
          {error}
        </div>
      )}

      {mappings.length > 0 && (
        <table className="w-full border-collapse text-left">
          <thead className="border-b bg-gray-50">
            <tr>
              <th className="px-4 py-2">Original</th>
              <th className="px-4 py-2">Detected Type</th>
              <th className="px-4 py-2">Suggested Match</th>
              <th className="px-4 py-2">Confidence</th>
            </tr>
          </thead>

          <tbody>
            {mappings.map((mapping) => (
              <tr key={mapping.original} className="border-b">
                <td className="px-4 py-2">{mapping.original}</td>
                <td className="px-4 py-2">
                  {getDetectedType(mapping.original)}
                </td>

                <td className="px-4 py-2">
                  <select
                    value={
                      localMappings.find(
                        (item) => item.original === mapping.original
                      )?.canonical || ""
                    }
                    onChange={(event) => {
                      setLocalMappings((prev) =>
                        prev.map((item) =>
                          item.original === mapping.original
                            ? { ...item, canonical: event.target.value }
                            : item
                        )
                      );
                    }}
                    className="w-full rounded border px-2 py-1"
                  >
                    <option value="">(skip)</option>
                    {selectedSchema.map((field) => (
                      <option key={field.name} value={field.name}>
                        {field.name}
                        {field.required ? " *" : ""}
                      </option>
                    ))}
                  </select>
                </td>

                <td className="px-4 py-2">
                  <span
                    className={`rounded px-2 py-1 text-sm ${getConfidenceClass(
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
        <div className="mt-6">
          {missingRequired && (
            <p className="mb-2 text-sm text-red-600">
              Required fields must be mapped before continuing.
            </p>
          )}

          {hasDuplicateTargets && (
            <p className="mb-2 text-sm text-red-600">
              Each target field can only be mapped once.
            </p>
          )}

          <button
            onClick={handleConfirm}
            disabled={confirmDisabled}
            className="rounded bg-slate-900 px-4 py-2 text-white disabled:cursor-not-allowed disabled:opacity-50"
          >
            {isSubmitting ? "Processing..." : "Confirm Mappings"}
          </button>
        </div>
      )}
    </div>
  );
}

export default MappingPage;