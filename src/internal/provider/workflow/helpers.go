// Copyright (c) 2024 Florent (Kodflow). All rights reserved.
// Licensed under the Sustainable Use License 1.0
// See LICENSE in the project root for license information.

// Package workflow implements workflow management resources and data sources.
package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/kodflow/terraform-provider-n8n/sdk/n8nsdk"
	"github.com/kodflow/terraform-provider-n8n/src/internal/provider/workflow/models"
)

// CALLER_POLICY_DEFAULT is the default value for the CallerPolicy workflow setting.
// The n8n API returns this value even when not explicitly set by the user.
const CALLER_POLICY_DEFAULT string = "workflowsFromSameOwner"

// SETTINGS_RESET_VALUE is the value n8n treats as "use the instance default" for a setting.
// Recent n8n versions delete it from stored settings on update; older versions store it
// as-is, which updateWorkflowWithResetCheck detects and reverts.
const SETTINGS_RESET_VALUE string = "DEFAULT"

// parseWorkflowJSON parses the JSON fields from a workflow model.
//
// Params:
//   - plan: The workflow resource model containing JSON data
//   - diags: Diagnostics for error reporting
//
// Returns:
//   - []n8nsdk.Node: Parsed workflow nodes
//   - map[string]any: Parsed workflow connections
//   - n8nsdk.WorkflowSettings: Parsed workflow settings
func parseWorkflowJSON(plan *models.Resource, diags *diag.Diagnostics) ([]n8nsdk.Node, map[string]any, n8nsdk.WorkflowSettings) {
	// Parse nodes
	var nodes []n8nsdk.Node
	// Check for non-nil value.
	if !plan.NodesJSON.IsNull() && !plan.NodesJSON.IsUnknown() {
		// Check for error.
		if err := json.Unmarshal([]byte(plan.NodesJSON.ValueString()), &nodes); err != nil {
			diags.AddError("Invalid nodes JSON", fmt.Sprintf("Could not parse nodes_json: %s", err.Error()))
			// Return failure status.
			return []n8nsdk.Node{}, map[string]any{}, n8nsdk.WorkflowSettings{}
		}
	} else {
		// Return empty slice.
		nodes = []n8nsdk.Node{}
	}

	// Parse connections
	var connections map[string]any
	// Check for non-nil value.
	if !plan.ConnectionsJSON.IsNull() && !plan.ConnectionsJSON.IsUnknown() {
		// Check for error.
		if err := json.Unmarshal([]byte(plan.ConnectionsJSON.ValueString()), &connections); err != nil {
			diags.AddError("Invalid connections JSON", fmt.Sprintf("Could not parse connections_json: %s", err.Error()))
			// Return failure status.
			return []n8nsdk.Node{}, map[string]any{}, n8nsdk.WorkflowSettings{}
		}
	} else {
		// Return empty slice.
		connections = map[string]any{}
	}

	// Parse settings
	var settings n8nsdk.WorkflowSettings
	// Check for non-nil value.
	if !plan.SettingsJSON.IsNull() && !plan.SettingsJSON.IsUnknown() {
		// Check for error.
		if err := json.Unmarshal([]byte(plan.SettingsJSON.ValueString()), &settings); err != nil {
			diags.AddError("Invalid settings JSON", fmt.Sprintf("Could not parse settings_json: %s", err.Error()))
			// Return failure status.
			return []n8nsdk.Node{}, map[string]any{}, n8nsdk.WorkflowSettings{}
		}
	}

	// Return result.
	return nodes, connections, settings
}

// mapTagsFromWorkflow maps tags from the SDK workflow to Terraform types.
//
// Params:
//   - ctx: Context for the API call
//   - workflow: The workflow from SDK containing tags
//   - diags: Diagnostics for error reporting
//
// Returns:
//   - types.Set: Terraform set of tag IDs
func mapTagsFromWorkflow(ctx context.Context, workflow *n8nsdk.Workflow, diags *diag.Diagnostics) types.Set {
	// Check length.
	if len(workflow.Tags) > 0 {
		// Collect tag IDs
		tagIDs := make([]types.String, 0, len(workflow.Tags))
		// Iterate over items.
		for _, tag := range workflow.Tags {
			// Check for non-nil value.
			if tag.Id != nil {
				tagIDs = append(tagIDs, types.StringValue(*tag.Id))
			}
		}

		tagSet, tagDiags := types.SetValueFrom(ctx, types.StringType, tagIDs)
		diags.Append(tagDiags...)
		// Return result.
		return tagSet
	}

	// Return null set if no tags to avoid inconsistent result errors.
	return types.SetNull(types.StringType)
}

// mapWorkflowBasicFields maps basic workflow fields to the model.
//
// Params:
//   - workflow: The n8n workflow to map from
//   - plan: The resource model to update
func mapWorkflowBasicFields(workflow *n8nsdk.Workflow, plan *models.Resource) {
	// Set active status if available.
	if workflow.Active != nil {
		plan.Active = types.BoolPointerValue(workflow.Active)
	}
	// Set version ID if available.
	if workflow.VersionId != nil {
		plan.VersionID = types.StringPointerValue(workflow.VersionId)
	}
	// Set archived status if available.
	if workflow.IsArchived != nil {
		plan.IsArchived = types.BoolPointerValue(workflow.IsArchived)
	}
	// Set trigger count if available.
	if workflow.TriggerCount != nil {
		plan.TriggerCount = types.Int64Value(int64(*workflow.TriggerCount))
	}
}

// mapWorkflowProjectID maps the workflow project ID from shared info to the model.
//
// Params:
//   - workflow: The n8n workflow to map from
//   - plan: The resource model to update
func mapWorkflowProjectID(workflow *n8nsdk.Workflow, plan *models.Resource) {
	// Extract projectId from workflow.Shared[0].ProjectId
	// Check if workflow has shared projects
	if len(workflow.Shared) > 0 {
		// Check if the first shared project has a project ID
		if workflow.Shared[0].ProjectId != nil {
			plan.ProjectID = types.StringPointerValue(workflow.Shared[0].ProjectId)
		} else {
			// Project ID is nil, set to null in state
			plan.ProjectID = types.StringNull()
		}
	} else {
		// No shared projects, workflow is in default location
		plan.ProjectID = types.StringNull()
	}
}

// mapWorkflowTimestamps maps workflow timestamps to the model.
//
// Params:
//   - workflow: The n8n workflow to map from
//   - plan: The resource model to update
func mapWorkflowTimestamps(workflow *n8nsdk.Workflow, plan *models.Resource) {
	// Set creation timestamp if available.
	if workflow.CreatedAt != nil {
		plan.CreatedAt = types.StringValue(workflow.CreatedAt.Format("2006-01-02T15:04:05Z07:00"))
	}
	// Set update timestamp if available.
	if workflow.UpdatedAt != nil {
		plan.UpdatedAt = types.StringValue(workflow.UpdatedAt.Format("2006-01-02T15:04:05Z07:00"))
	}
}

// preserveProjectIDOnUpdate preserves the project_id after update.
// The PUT API doesn't return Shared data, so mapWorkflowToModel sets project_id to null.
// This function restores the correct project_id from plan/state.
//
// Params:
//   - plan: The planned resource data (will be updated)
//   - state: The current resource state
func preserveProjectIDOnUpdate(plan, state *models.Resource) {
	// Check if project changed
	if !plan.ProjectID.Equal(state.ProjectID) {
		// Project was changed, but if plan is null/unknown, keep state value
		// (removing from project is not supported by n8n API)
		if plan.ProjectID.IsNull() || plan.ProjectID.IsUnknown() {
			plan.ProjectID = state.ProjectID
		}
		// Otherwise keep plan value (already set by handleProjectAssignment)
	} else {
		// No change requested, preserve state value since API didn't return it
		plan.ProjectID = state.ProjectID
	}
}

// mapWorkflowToModel maps a workflow from the SDK to the Terraform model.
// This updates computed fields like timestamps, version, metadata, etc.
//
// Params:
//   - ctx: Context for the API call
//   - workflow: The workflow from SDK to map
//   - plan: The Terraform model to update
//   - diags: Diagnostics for error reporting
func mapWorkflowToModel(ctx context.Context, workflow *n8nsdk.Workflow, plan *models.Resource, diags *diag.Diagnostics) {
	// Basic fields
	plan.Name = types.StringValue(workflow.Name)

	// Map simple fields
	mapWorkflowBasicFields(workflow, plan)

	// Tags
	plan.Tags = mapTagsFromWorkflow(ctx, workflow, diags)

	// Project ID from shared workflow info
	mapWorkflowProjectID(workflow, plan)

	// Map timestamps
	mapWorkflowTimestamps(workflow, plan)

	// Map objects
	// Check for non-nil value.
	if workflow.Meta != nil {
		metaMap, metaDiags := types.MapValueFrom(ctx, types.StringType, workflow.Meta)
		diags.Append(metaDiags...)
		// Check condition.
		if !diags.HasError() {
			plan.Meta = metaMap
		}
	} else {
		// Set null map when API returns nil to ensure attribute is known.
		plan.Meta = types.MapNull(types.StringType)
	}
	// Check for non-nil value.
	if workflow.PinData != nil {
		pinDataMap, pinDiags := types.MapValueFrom(ctx, types.StringType, workflow.PinData)
		diags.Append(pinDiags...)
		// Check condition.
		if !diags.HasError() {
			plan.PinData = pinDataMap
		}
	} else {
		// Set null map when API returns nil to ensure attribute is known.
		plan.PinData = types.MapNull(types.StringType)
	}

	// Serialize JSON fields
	serializeWorkflowJSON(workflow, plan)
}

// serializeWorkflowJSON serializes workflow nodes, connections and settings back to JSON strings.
//
// Params:
//   - workflow: The workflow from SDK containing data to serialize
//   - plan: The Terraform model to update with serialized JSON
//
// Returns:
//   - None: Updates plan in-place
func serializeWorkflowJSON(workflow *n8nsdk.Workflow, plan *models.Resource) {
	// Check for non-nil value.
	if workflow.Nodes != nil {
		// Check for error.
		if nodesJSON, err := json.Marshal(workflow.Nodes); err == nil {
			plan.NodesJSON = types.StringValue(string(nodesJSON))
		}
	}
	// Check for non-nil value.
	if workflow.Connections != nil {
		// Check for error.
		if connectionsJSON, err := json.Marshal(workflow.Connections); err == nil {
			plan.ConnectionsJSON = types.StringValue(string(connectionsJSON))
		}
	}
	// Normalize settings before serialization to avoid unnecessary diffs.
	normalizedSettings := normalizeWorkflowSettings(workflow.Settings)
	// Keep a "DEFAULT" declared in settings_json that n8n deleted on update.
	preserveDeclaredResetValues(&normalizedSettings, plan.SettingsJSON)
	// Check for error.
	if settingsJSON, err := marshalSettingsWithDeclaredNulls(normalizedSettings, plan.SettingsJSON); err == nil {
		plan.SettingsJSON = types.StringValue(string(settingsJSON))
	}
}

// marshalSettingsWithDeclaredNulls serializes settings and keeps the keys that settings_json
// declares as null when n8n no longer returns them. settings_json is a plain string, so a
// removed key written as null must stay in state for the value to match the configuration.
// A key declared as null that n8n still returns is serialized with its value.
//
// Params:
//   - settings: Normalized settings returned by the API
//   - configuredJSON: settings_json value from the plan or prior state
//
// Returns:
//   - []byte: Serialized settings
//   - error: Serialization error
func marshalSettingsWithDeclaredNulls(settings n8nsdk.WorkflowSettings, configuredJSON types.String) ([]byte, error) {
	serialized, err := settings.ToMap()
	// Check for serialization error.
	if err != nil {
		return nil, err
	}
	// Keep each null marker for a key n8n did not return.
	for _, key := range declaredNullSettingKeys(configuredJSON) {
		// Skip keys n8n still returns.
		if _, returned := serialized[key]; !returned {
			serialized[key] = nil
		}
	}

	// Return serialized settings.
	return json.Marshal(serialized)
}

// declaredNullSettingKeys returns the keys that settings_json declares as null.
//
// Params:
//   - configuredJSON: settings_json value from the plan or prior state
//
// Returns:
//   - []string: Keys set to null, empty when the value is null, unknown or invalid
func declaredNullSettingKeys(configuredJSON types.String) []string {
	var configured map[string]any
	// Null, unknown or invalid settings_json declares no key.
	if err := json.Unmarshal([]byte(configuredJSON.ValueString()), &configured); err != nil {
		return nil
	}

	keys := make([]string, 0, len(configured))
	// Collect keys whose value is null.
	for key, value := range configured {
		// Keep only null values.
		if value == nil {
			keys = append(keys, key)
		}
	}

	// Return null keys.
	return keys
}

// normalizeWorkflowSettings removes default values from settings.
// The n8n API returns default values for certain settings even when not explicitly set.
// This function removes callerPolicy and availableInMCP defaults to match user config.
//
// Params:
//   - settings: Original workflow settings from API
//
// Returns:
//   - n8nsdk.WorkflowSettings: Normalized settings without default values
func normalizeWorkflowSettings(settings n8nsdk.WorkflowSettings) n8nsdk.WorkflowSettings {
	// Create a copy to avoid modifying the original.
	normalized := settings

	// Remove callerPolicy if it's the default value.
	if normalized.CallerPolicy != nil && *normalized.CallerPolicy == CALLER_POLICY_DEFAULT {
		normalized.CallerPolicy = nil
	}

	// Remove availableInMCP if it's the default value (false).
	if normalized.AvailableInMCP != nil && !*normalized.AvailableInMCP {
		normalized.AvailableInMCP = nil
	}

	// Return result.
	return normalized
}

// parseWorkflowUpdateJSON parses the plan JSON fields for an update request.
// Settings removed from settings_json are reset, since n8n merges settings on update.
//
// Params:
//   - plan: The planned resource data
//   - state: The current resource state
//   - diags: Diagnostics for error reporting
//
// Returns:
//   - []n8nsdk.Node: Parsed workflow nodes
//   - map[string]any: Parsed workflow connections
//   - n8nsdk.WorkflowSettings: Parsed workflow settings with removed keys reset
func parseWorkflowUpdateJSON(plan, state *models.Resource, diags *diag.Diagnostics) ([]n8nsdk.Node, map[string]any, n8nsdk.WorkflowSettings) {
	nodes, connections, settings := parseWorkflowJSON(plan, diags)
	// Check for JSON parsing errors.
	if diags.HasError() {
		return nodes, connections, settings
	}

	resetRemovedWorkflowSettings(&settings, plan.SettingsJSON, state.SettingsJSON, resettableSettingKeys(plan), diags)

	// Return parsed fields.
	return nodes, connections, settings
}

// resetRemovedWorkflowSettings marks settings removed from settings_json so n8n drops them.
// The n8n API merges the submitted settings into the stored ones on update, so omitting a key
// keeps its previous value and the API response no longer matches the plan, which causes
// "Provider produced inconsistent result after apply". Each key present in state but absent
// from the plan is sent with a value that n8n deletes ("DEFAULT") or that normalization removes
// from state (default callerPolicy, availableInMCP=false). Keys the public API cannot reset
// produce an error before any request is sent.
//
// Params:
//   - settings: Settings parsed from the plan, updated in place
//   - planSettingsJSON: settings_json value from the plan
//   - stateSettingsJSON: settings_json value from the current state
//   - resettable: Keys that may be reset (see resettableSettingKeys)
//   - diags: Diagnostics for error reporting
func resetRemovedWorkflowSettings(settings *n8nsdk.WorkflowSettings, planSettingsJSON, stateSettingsJSON types.String, resettable []string, diags *diag.Diagnostics) {
	removed, err := removedWorkflowSettingKeys(*settings, planSettingsJSON, stateSettingsJSON)
	// Check for serialization error.
	if err != nil {
		diags.AddError("Invalid settings JSON", fmt.Sprintf("Could not serialize settings_json: %s", err.Error()))
		return
	}

	unsupported := make([]string, 0, len(removed))
	// Reset every removed key, keeping the ones the API cannot reset.
	for _, key := range removed {
		// Collect keys without a reset value for the error below.
		if !slices.Contains(resettable, key) || !resetWorkflowSetting(settings, key) {
			unsupported = append(unsupported, key)
		}
	}

	// Fail before the update when a removed key cannot be reset through the API.
	if len(unsupported) > 0 {
		diags.AddError(
			"Workflow settings cannot be removed",
			fmt.Sprintf(
				"The settings [%s] are stored in n8n but missing from settings_json. "+
					"The n8n API merges settings on update and cannot reset these keys. "+
					"Declare them in settings_json or reset them in the n8n UI.%s",
				strings.Join(unsupported, ", "), errorWorkflowResetHint(unsupported),
			),
		)
	}
}

// removedWorkflowSettingKeys returns the sorted settings keys present in state but absent from the plan.
//
// Params:
//   - settings: Settings parsed from the plan
//   - planSettingsJSON: settings_json value from the plan
//   - stateSettingsJSON: settings_json value from the current state
//
// Returns:
//   - []string: Removed keys, sorted; nil when plan or state settings are not known
//   - error: Serialization error of the plan settings
func removedWorkflowSettingKeys(settings n8nsdk.WorkflowSettings, planSettingsJSON, stateSettingsJSON types.String) ([]string, error) {
	// Without known plan and state values there is no removal to detect.
	if !isKnownString(planSettingsJSON) || !isKnownString(stateSettingsJSON) {
		return nil, nil
	}

	var stateSettings map[string]any
	// State is written by the provider; ignore content that is not a JSON object.
	if err := json.Unmarshal([]byte(stateSettingsJSON.ValueString()), &stateSettings); err != nil {
		return nil, nil
	}

	planSettings, err := settings.ToMap()
	// Check for serialization error.
	if err != nil {
		return nil, err
	}

	removed := make([]string, 0, len(stateSettings))
	// Collect every key stored in state that the plan no longer declares.
	for key, value := range stateSettings {
		// Keep only keys with a stored value that are missing from the plan; null means absent.
		if _, declared := planSettings[key]; !declared && value != nil {
			removed = append(removed, key)
		}
	}
	slices.Sort(removed)

	// Return sorted keys.
	return removed, nil
}

// resettableSettingKeys returns the settings keys the provider may reset for a plan.
// errorWorkflow is included only with allow_error_workflow_reset, since n8n versions
// older than 2.5 store "DEFAULT" as a workflow ID and skip the workflow's Error Trigger.
//
// Params:
//   - plan: The planned resource data
//
// Returns:
//   - []string: Keys that may be reset when removed from settings_json
func resettableSettingKeys(plan *models.Resource) []string {
	keys := []string{settingTimezone, "callerPolicy", "availableInMCP"}
	// Opt in to the errorWorkflow reset.
	if plan.AllowErrorWorkflowReset.ValueBool() {
		keys = append(keys, settingErrorWorkflow)
	}

	// Return resettable keys.
	return keys
}

// resetWorkflowSetting sets the reset value accepted by the public API for a removed key.
//
// Params:
//   - settings: Settings sent in the update request, updated in place
//   - key: Removed settings key
//
// Returns:
//   - bool: False when the key cannot be reset through the public API
func resetWorkflowSetting(settings *n8nsdk.WorkflowSettings, key string) bool {
	// Pick the value n8n deletes or normalization removes for this key.
	switch key {
	case settingErrorWorkflow:
		settings.SetErrorWorkflow(SETTINGS_RESET_VALUE)
	case settingTimezone:
		settings.SetTimezone(SETTINGS_RESET_VALUE)
	case "callerPolicy":
		settings.SetCallerPolicy(CALLER_POLICY_DEFAULT)
	case "availableInMCP":
		settings.SetAvailableInMCP(false)
	default:
		// Booleans, enums and numbers reject "DEFAULT" in the public API schema.
		return false
	}

	// Return success.
	return true
}

// isKnownString reports whether a string value is neither null nor unknown.
//
// Params:
//   - value: Terraform string value
//
// Returns:
//   - bool: True when the value is known and not null
func isKnownString(value types.String) bool {
	// Return known state.
	return !value.IsNull() && !value.IsUnknown()
}

// resetSettingKeys returns the settings keys that are reset by sending SETTINGS_RESET_VALUE.
//
// Returns:
//   - []string: Settings keys reset with "DEFAULT"
func resetSettingKeys() []string {
	// Return keys reset with "DEFAULT".
	return []string{settingErrorWorkflow, settingTimezone}
}

// resetSettingMinVersion returns the first n8n version that deletes "DEFAULT" for a key
// when the workflow is updated through the public API.
//
// Params:
//   - key: Settings key reset with "DEFAULT"
//
// Returns:
//   - string: Minimum n8n version, empty for unknown keys
func resetSettingMinVersion(key string) string {
	// Pick the version that added server-side cleanup for this key.
	switch key {
	case settingErrorWorkflow:
		return "2.5"
	case settingTimezone:
		return "1.123"
	default:
		return ""
	}
}

// resetSettingField returns the settings field of a key reset with "DEFAULT".
//
// Params:
//   - settings: Workflow settings
//   - key: Settings key reset with "DEFAULT"
//
// Returns:
//   - **string: Address of the field, nil for keys not reset with "DEFAULT"
func resetSettingField(settings *n8nsdk.WorkflowSettings, key string) **string {
	// Map the key to its struct field.
	switch key {
	case settingErrorWorkflow:
		return &settings.ErrorWorkflow
	case settingTimezone:
		return &settings.Timezone
	default:
		return nil
	}
}

// isSettingsResetValue reports whether a setting value is SETTINGS_RESET_VALUE.
//
// Params:
//   - value: Setting value
//
// Returns:
//   - bool: True when the value is "DEFAULT"
func isSettingsResetValue(value *string) bool {
	// Return reset state.
	return value != nil && *value == SETTINGS_RESET_VALUE
}

// declaredSettings decodes settings_json into a map of declared keys.
// Keys set to null are not declared: parsing drops them, so they count as removed.
//
// Params:
//   - settingsJSON: settings_json value from the plan or state
//
// Returns:
//   - map[string]any: Declared non-null settings, empty when the value is null, unknown or invalid
func declaredSettings(settingsJSON types.String) map[string]any {
	declared := map[string]any{}
	// Null, unknown or invalid settings_json declares no key.
	if err := json.Unmarshal([]byte(settingsJSON.ValueString()), &declared); err != nil {
		return map[string]any{}
	}
	// Drop keys set to null.
	maps.DeleteFunc(declared, func(_ string, value any) bool { return value == nil })

	// Return declared keys.
	return declared
}

// providerResetKeys returns the keys the provider reset with "DEFAULT" in the update request.
// Keys declared in settings_json are skipped, since their "DEFAULT" comes from the configuration.
//
// Params:
//   - sent: Settings sent in the update request
//   - planSettingsJSON: settings_json value from the plan
//
// Returns:
//   - []string: Keys reset by the provider
func providerResetKeys(sent n8nsdk.WorkflowSettings, planSettingsJSON types.String) []string {
	declared := declaredSettings(planSettingsJSON)
	keys := make([]string, 0, len(resetSettingKeys()))
	// Check every key that may have been reset by the provider.
	for _, key := range resetSettingKeys() {
		_, isDeclared := declared[key]
		// Keep undeclared keys sent as "DEFAULT".
		if !isDeclared && isSettingsResetValue(*resetSettingField(&sent, key)) {
			keys = append(keys, key)
		}
	}

	// Return reset keys.
	return keys
}

// retainedResetSettings returns the keys reset with "DEFAULT" that n8n stored as-is.
//
// Params:
//   - sent: Settings sent in the update request
//   - returned: Settings returned by the API after the update
//   - planSettingsJSON: settings_json value from the plan
//
// Returns:
//   - []string: Keys n8n kept as "DEFAULT" instead of deleting them
func retainedResetSettings(sent, returned n8nsdk.WorkflowSettings, planSettingsJSON types.String) []string {
	resets := providerResetKeys(sent, planSettingsJSON)
	retained := make([]string, 0, len(resets))
	// Keep reset keys that came back unchanged.
	for _, key := range resets {
		// Check the value returned by the API.
		if isSettingsResetValue(*resetSettingField(&returned, key)) {
			retained = append(retained, key)
		}
	}

	// Return retained keys.
	return retained
}

// updateWorkflowWithResetCheck updates the workflow and reverts resets the n8n version stored as-is.
// n8n versions without server-side cleanup keep "DEFAULT" literally, which breaks the timezone
// and the error workflow at runtime. In that case the previous values are restored with a second
// request and an error explains the minimum n8n version required for the removal.
//
// Params:
//   - ctx: Context for the API calls
//   - workflowID: The ID of the workflow to update
//   - request: The workflow updates to apply
//   - plan: The planned resource data
//   - state: The current resource state
//   - diags: Diagnostics for error reporting
//
// Returns:
//   - *n8nsdk.Workflow: Updated workflow, nil on error or when a reset was reverted
func (r *WorkflowResource) updateWorkflowWithResetCheck(ctx context.Context, workflowID string, request n8nsdk.Workflow, plan, state *models.Resource, diags *diag.Diagnostics) *n8nsdk.Workflow {
	workflow := r.updateWorkflowViaAPI(ctx, workflowID, request, diags)
	// Check for API error.
	if workflow == nil {
		warnUnverifiedResets(providerResetKeys(request.Settings, plan.SettingsJSON), diags)
		return nil
	}

	retained := retainedResetSettings(request.Settings, workflow.Settings, plan.SettingsJSON)
	// Nothing to revert when n8n deleted every reset key.
	if len(retained) == 0 {
		return workflow
	}

	r.restoreRetainedSettings(ctx, workflowID, request, retained, state.SettingsJSON, diags)

	// Return nil so the previous state is kept.
	return nil
}

// restoreRetainedSettings restores the state values of keys n8n kept as "DEFAULT" and reports the error.
// The restoration is confirmed from the response of the second request; when it cannot be
// confirmed, the diagnostic asks for a manual fix in the n8n UI.
//
// Params:
//   - ctx: Context for the API call
//   - workflowID: The ID of the workflow to update
//   - request: The workflow update that was applied
//   - retained: Keys n8n kept as "DEFAULT"
//   - stateSettingsJSON: settings_json value from the current state
//   - diags: Diagnostics for error reporting
func (r *WorkflowResource) restoreRetainedSettings(ctx context.Context, workflowID string, request n8nsdk.Workflow, retained []string, stateSettingsJSON types.String, diags *diag.Diagnostics) {
	previous := declaredSettings(stateSettingsJSON)
	// Put back the value stored before the update for each retained key.
	for _, key := range retained {
		// Restore only string values recorded in state.
		if value, ok := previous[key].(string); ok {
			*resetSettingField(&request.Settings, key) = n8nsdk.PtrString(value)
		}
	}

	restored := r.updateWorkflowViaAPI(ctx, workflowID, request, diags)
	outcome := "The previous values were restored."
	// Report a restoration that the API response does not confirm.
	if !settingsRestored(restored, request.Settings, retained) {
		outcome = fmt.Sprintf("Restoring the previous values could not be confirmed, so n8n may still use %q "+
			"for these settings: set them manually in the n8n UI.", SETTINGS_RESET_VALUE)
	}

	diags.AddError(
		"Workflow settings cannot be removed on this n8n version",
		fmt.Sprintf(
			"n8n stored [%s] as %q instead of removing them, so this n8n version cannot remove them "+
				"through the public API (%s). %s Declare them in settings_json or reset them in the n8n UI.",
			strings.Join(retained, ", "), SETTINGS_RESET_VALUE, resetRequirements(retained), outcome,
		),
	)
}

// settingsRestored reports whether the API response confirms the restored values.
//
// Params:
//   - workflow: Workflow returned by the restore request, nil when it failed
//   - sent: Settings sent in the restore request
//   - keys: Restored keys
//
// Returns:
//   - bool: True when every key came back with the restored value
func settingsRestored(workflow *n8nsdk.Workflow, sent n8nsdk.WorkflowSettings, keys []string) bool {
	// A failed request confirms nothing.
	if workflow == nil {
		return false
	}
	// Compare every restored key with the value sent.
	for _, key := range keys {
		want, got := *resetSettingField(&sent, key), *resetSettingField(&workflow.Settings, key)
		// Fail when the value is still "DEFAULT" or differs from the one sent.
		if isSettingsResetValue(got) || got == nil || want == nil || *got != *want {
			return false
		}
	}

	// Return confirmed restoration.
	return true
}

// resetRequirements describes the minimum n8n version required to remove each key.
//
// Params:
//   - keys: Keys reset with "DEFAULT"
//
// Returns:
//   - string: Comma-separated version requirements
func resetRequirements(keys []string) string {
	requirements := make([]string, 0, len(keys))
	// Describe the requirement of each key.
	for _, key := range keys {
		requirements = append(requirements, fmt.Sprintf("%s requires n8n >= %s", key, resetSettingMinVersion(key)))
	}

	// Return joined requirements.
	return strings.Join(requirements, ", ")
}

// errorWorkflowResetHint explains how to opt in to the errorWorkflow reset.
//
// Params:
//   - unsupported: Removed keys that cannot be reset
//
// Returns:
//   - string: Hint appended to the diagnostic, empty when errorWorkflow is not listed
func errorWorkflowResetHint(unsupported []string) string {
	// Only errorWorkflow can be reset through the opt-in.
	if !slices.Contains(unsupported, settingErrorWorkflow) {
		return ""
	}

	// Return the opt-in hint.
	return fmt.Sprintf(" To reset errorWorkflow on n8n >= %s, set allow_error_workflow_reset = true.", resetSettingMinVersion(settingErrorWorkflow))
}

// warnUnverifiedResets warns that resets may have been applied when the update response is missing.
// n8n may have stored the update before the error, and older versions keep "DEFAULT" as-is.
//
// Params:
//   - keys: Keys reset by the provider in the failed request
//   - diags: Diagnostics for error reporting
func warnUnverifiedResets(keys []string, diags *diag.Diagnostics) {
	// Nothing to verify without provider resets.
	if len(keys) == 0 {
		return
	}

	diags.AddWarning(
		"Workflow settings reset could not be verified",
		fmt.Sprintf(
			"The update request reset [%s] to %q, but no response was received, so the result could not be checked. "+
				"n8n may have applied the update; older versions store %q as-is (%s). Check these settings in the n8n UI.",
			strings.Join(keys, ", "), SETTINGS_RESET_VALUE, SETTINGS_RESET_VALUE, resetRequirements(keys),
		),
	)
}

// preserveDeclaredResetValues keeps a "DEFAULT" declared in settings_json when n8n deleted it.
// n8n treats "DEFAULT" as an absent key, so both representations are equivalent; keeping the
// declared one makes the state match the configuration.
//
// Params:
//   - settings: Normalized settings written to state, updated in place
//   - configuredJSON: settings_json value from the plan or prior state
func preserveDeclaredResetValues(settings *n8nsdk.WorkflowSettings, configuredJSON types.String) {
	declared := declaredSettings(configuredJSON)
	// Check every key that accepts "DEFAULT".
	for _, key := range resetSettingKeys() {
		field := resetSettingField(settings, key)
		// Restore the declared value only when n8n omitted the key.
		if declared[key] == SETTINGS_RESET_VALUE && *field == nil {
			*field = n8nsdk.PtrString(SETTINGS_RESET_VALUE)
		}
	}
}

// convertTagIDsToTagIdsInner converts string tag IDs to SDK TagIdsInner format.
//
// Params:
//   - tagIDs: List of tag ID strings
//
// Returns:
//   - []n8nsdk.TagIdsInner: Converted tag IDs in SDK format
func convertTagIDsToTagIdsInner(tagIDs []string) []n8nsdk.TagIdsInner {
	tagIdsInner := make([]n8nsdk.TagIdsInner, 0, len(tagIDs))
	// Iterate over items.
	for _, tagID := range tagIDs {
		tagIdsInner = append(tagIdsInner, n8nsdk.TagIdsInner{Id: tagID})
	}
	// Return result.
	return tagIdsInner
}

// handleWorkflowActivation handles the activation/deactivation of a workflow.
//
// Params:
//   - ctx: Context for the API call
//   - plan: The desired workflow state
//   - state: The current workflow state
//   - diags: Diagnostics for error reporting
//
// Returns:
//   - None: Updates workflow via API
func (r *WorkflowResource) handleWorkflowActivation(ctx context.Context, plan, state *models.Resource, diags *diag.Diagnostics) {
	activeChanged := isActivationChanged(plan, state)

	// Check condition.
	if !activeChanged {
		// Return success status.
		return
	}

	var httpResp *http.Response
	var err error

	// Use dedicated activate/deactivate endpoints
	// Check condition.
	if plan.Active.ValueBool() {
		_, httpResp, err = r.client.APIClient.WorkflowAPI.WorkflowsIdActivatePost(ctx, plan.ID.ValueString()).Execute()
	} else {
		// Deactivate workflow when Active is false
		_, httpResp, err = r.client.APIClient.WorkflowAPI.WorkflowsIdDeactivatePost(ctx, plan.ID.ValueString()).Execute()
	}

	// Check for non-nil value.
	if httpResp != nil && httpResp.Body != nil {
		defer httpResp.Body.Close()
	}

	// Check for error.
	if err != nil {
		action := getActivationAction(plan)
		diags.AddError(
			fmt.Sprintf("Error changing workflow activation status to %s", action),
			fmt.Sprintf("Could not %s workflow ID %s: %s\nHTTP Response: %v", action, plan.ID.ValueString(), err.Error(), httpResp),
		)
	}
}

// isActivationChanged checks if the workflow activation status has changed.
//
// Params:
//   - plan: The desired workflow state
//   - state: The current workflow state
//
// Returns:
//   - bool: True if activation status has changed
func isActivationChanged(plan, state *models.Resource) bool {
	// Return boolean result.
	return !plan.Active.IsNull() && !plan.Active.IsUnknown() &&
		!state.Active.IsNull() && !state.Active.IsUnknown() &&
		plan.Active.ValueBool() != state.Active.ValueBool()
}

// getActivationAction returns the activation action string.
//
// Params:
//   - plan: The desired workflow state
//
// Returns:
//   - string: "activate" or "deactivate"
func getActivationAction(plan *models.Resource) string {
	// Check condition.
	if plan.Active.ValueBool() {
		// Return activate action.
		return "activate"
	}
	// Return deactivate action.
	return "deactivate"
}

// updateWorkflowTags updates the tags for a workflow.
//
// Params:
//   - ctx: Context for the API call
//   - workflowID: The workflow ID to update tags for
//   - plan: The Terraform model containing desired tags
//   - workflow: The SDK workflow to update
//   - diags: Diagnostics for error reporting
//
// Returns:
//   - None: Updates workflow tags via API
func (r *WorkflowResource) updateWorkflowTags(ctx context.Context, workflowID string, plan *models.Resource, workflow *n8nsdk.Workflow, diags *diag.Diagnostics) {
	// Check for null value.
	if plan.Tags.IsNull() || plan.Tags.IsUnknown() {
		// Return success status.
		return
	}

	var tagIDs []string
	diags.Append(plan.Tags.ElementsAs(ctx, &tagIDs, false)...)
	// Check condition.
	if diags.HasError() {
		// Return failure status.
		return
	}

	tagIdsInner := convertTagIDsToTagIdsInner(tagIDs)

	tags, httpResp, err := r.client.APIClient.WorkflowAPI.WorkflowsIdTagsPut(ctx, workflowID).
		TagIdsInner(tagIdsInner).
		Execute()

	// Check for non-nil value.
	if httpResp != nil && httpResp.Body != nil {
		defer httpResp.Body.Close()
	}

	// Check for error.
	if err != nil {
		diags.AddError(
			"Error updating workflow tags",
			fmt.Sprintf("Could not update tags for workflow ID %s: %s\nHTTP Response: %v", workflowID, err.Error(), httpResp),
		)
		// Return failure status.
		return
	}

	workflow.Tags = tags
}

// createWorkflowViaAPI creates a new workflow via the n8n API.
//
// Params:
//   - ctx: Context for the API call
//   - workflowRequest: The workflow to create
//   - diags: Diagnostics for error reporting
//
// Returns:
//   - *n8nsdk.Workflow: Created workflow if successful, nil otherwise
func (r *WorkflowResource) createWorkflowViaAPI(ctx context.Context, workflowRequest n8nsdk.Workflow, diags *diag.Diagnostics) *n8nsdk.Workflow {
	workflow, httpResp, err := r.client.APIClient.WorkflowAPI.WorkflowsPost(ctx).
		Workflow(workflowRequest).
		Execute()

	// Check for non-nil HTTP response
	if httpResp != nil && httpResp.Body != nil {
		defer httpResp.Body.Close()
	}

	// Check for API error
	if err != nil {
		diags.AddError(
			"Error creating workflow",
			fmt.Sprintf("Could not create workflow, unexpected error: %s\nHTTP Response: %v", err.Error(), httpResp),
		)
		return nil
	}

	// Return the created workflow
	return workflow
}

// updateWorkflowViaAPI updates an existing workflow via the n8n API.
//
// Params:
//   - ctx: Context for the API call
//   - workflowID: The ID of the workflow to update
//   - workflowRequest: The workflow updates to apply
//   - diags: Diagnostics for error reporting
//
// Returns:
//   - *n8nsdk.Workflow: Updated workflow if successful, nil otherwise
func (r *WorkflowResource) updateWorkflowViaAPI(ctx context.Context, workflowID string, workflowRequest n8nsdk.Workflow, diags *diag.Diagnostics) *n8nsdk.Workflow {
	workflow, httpResp, err := r.client.APIClient.WorkflowAPI.WorkflowsIdPut(ctx, workflowID).
		Workflow(workflowRequest).
		Execute()

	// Check for non-nil HTTP response
	if httpResp != nil && httpResp.Body != nil {
		defer httpResp.Body.Close()
	}

	// Check for API error
	if err != nil {
		diags.AddError(
			"Error updating workflow",
			fmt.Sprintf("Could not update workflow ID %s: %s\nHTTP Response: %v", workflowID, err.Error(), httpResp),
		)
		return nil
	}

	// Return the updated workflow
	return workflow
}

// transferWorkflowToProject transfers a workflow to a specified project.
//
// Params:
//   - ctx: Context for the API call
//   - workflowID: The workflow ID to transfer
//   - projectID: The destination project ID
//   - diags: Diagnostics for error reporting
//
// Returns:
//   - bool: True if transfer succeeded, false otherwise
func (r *WorkflowResource) transferWorkflowToProject(ctx context.Context, workflowID, projectID string, diags *diag.Diagnostics) bool {
	transferRequest := n8nsdk.WorkflowsIdTransferPutRequest{
		DestinationProjectId: projectID,
	}

	httpResp, err := r.client.APIClient.WorkflowAPI.
		WorkflowsIdTransferPut(ctx, workflowID).
		WorkflowsIdTransferPutRequest(transferRequest).
		Execute()

	// Check for non-nil HTTP response.
	if httpResp != nil && httpResp.Body != nil {
		defer httpResp.Body.Close()
	}

	// Check for error.
	if err != nil {
		diags.AddError(
			"Error transferring workflow to project",
			fmt.Sprintf("Could not transfer workflow ID %s to project %s: %s\nHTTP Response: %v", workflowID, projectID, err.Error(), httpResp),
		)
		// Return failure.
		return false
	}

	// Return success.
	return true
}

// handlePostCreation handles post-creation workflow operations (ID, tags, project, activation).
// This helper reduces complexity in the create operation by consolidating post-creation steps.
//
// Params:
//   - ctx: Context for the API call
//   - workflow: The created workflow
//   - plan: The resource model
//   - diags: Diagnostics for error reporting
//
// Returns:
//   - *n8nsdk.Workflow: Updated workflow if successful, nil otherwise
func (r *WorkflowResource) handlePostCreation(ctx context.Context, workflow *n8nsdk.Workflow, plan *models.Resource, diags *diag.Diagnostics) *n8nsdk.Workflow {
	// Set ID from created workflow
	plan.ID = types.StringPointerValue(workflow.Id)

	// Update tags if provided
	workflow = r.applyPostCreationTagsAndProject(ctx, workflow, plan, diags)
	// Check for tag/project errors
	if workflow == nil {
		return nil
	}

	// Handle workflow activation after creation if requested
	if !r.handlePostCreationActivation(ctx, plan, workflow, diags) {
		return nil
	}

	// Return the processed workflow with all post-creation operations applied
	return workflow
}

// applyPostCreationTagsAndProject applies tags and project assignment to a newly created workflow.
//
// Params:
//   - ctx: Context for the API call
//   - workflow: The created workflow
//   - plan: The resource model
//   - diags: Diagnostics for error reporting
//
// Returns:
//   - *n8nsdk.Workflow: Updated workflow if successful, nil otherwise
func (r *WorkflowResource) applyPostCreationTagsAndProject(ctx context.Context, workflow *n8nsdk.Workflow, plan *models.Resource, diags *diag.Diagnostics) *n8nsdk.Workflow {
	// Update tags if provided
	if !plan.Tags.IsNull() && !plan.Tags.IsUnknown() && workflow.Id != nil {
		r.updateWorkflowTags(ctx, *workflow.Id, plan, workflow, diags)
		// Check for tag update errors
		if diags.HasError() {
			return nil
		}
	}

	// Transfer workflow to project if project_id is specified
	if !plan.ProjectID.IsNull() && !plan.ProjectID.IsUnknown() && workflow.Id != nil {
		updatedWorkflow := r.handleProjectAssignment(ctx, *workflow.Id, plan.ProjectID.ValueString(), diags)
		// Check if project assignment succeeded
		if diags.HasError() {
			return nil
		}
		// Use updated workflow if available
		if updatedWorkflow != nil {
			workflow = updatedWorkflow
		}
	}

	// Return the workflow with tags and project applied
	return workflow
}

// handleProjectAssignment handles transferring a workflow to a project and re-fetching the workflow.
// This helper reduces complexity in create and update operations.
//
// Params:
//   - ctx: Context for the API call
//   - workflowID: The workflow ID to transfer
//   - projectID: The destination project ID
//   - diags: Diagnostics for error reporting
//
// Returns:
//   - *n8nsdk.Workflow: Updated workflow if successful, nil otherwise
func (r *WorkflowResource) handleProjectAssignment(ctx context.Context, workflowID, projectID string, diags *diag.Diagnostics) *n8nsdk.Workflow {
	// Transfer workflow to project
	if !r.transferWorkflowToProject(ctx, workflowID, projectID, diags) {
		return nil
	}

	// Re-fetch workflow to get updated project info
	workflow, httpResp, err := r.client.APIClient.WorkflowAPI.
		WorkflowsIdGet(ctx, workflowID).
		Execute()

	// Check for non-nil HTTP response.
	if httpResp != nil && httpResp.Body != nil {
		defer httpResp.Body.Close()
	}

	// Return nil on re-fetch failure; add diagnostic since transfer
	// succeeded but workflow couldn't be re-fetched
	if err != nil {
		diags.AddError(
			"Error re-fetching workflow after project transfer",
			fmt.Sprintf("Workflow was transferred to project successfully, but failed to re-fetch workflow ID %s: %s\nHTTP Response: %v", workflowID, err.Error(), httpResp),
		)
		return nil
	}

	// Return the updated workflow with project information
	return workflow
}
