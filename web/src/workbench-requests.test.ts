import {describe,it,expect} from 'vitest';
import {componentConfiguration,IdempotencyDraft,parseManagedConfiguration} from './workbench-requests';
describe('managed component request boundaries',()=>{
 it('preserves source bindings, other components and directive fields during a version change',()=>{
  const original={sources:[{id:'source-1'}],components:{beat:{desired_version:'1',health:'existing'},agent:{desired_version:'2'}},credential_env:'SOURCE_CREDENTIAL'};
  const result=componentConfiguration(JSON.stringify(original),'beat','3');
  expect(result).toEqual({...original,components:{...original.components,beat:{desired_version:'3',health:'existing'}}});
  expect(original.components.beat.desired_version).toBe('1');
 });
 it('rejects inline secrets and executable directives before a configuration leaves the browser',()=>{
  for(const raw of ['{"sources":[{"password":"secret"}]}','{"components":{"beat":{"command":"run"}}}','{"token_env":"not a variable"}','{"components":[]}'])expect(()=>parseManagedConfiguration(raw)).toThrow();
 });
 it('keeps a retry idempotent and creates a new request identity after editing',()=>{
  const request=new IdempotencyDraft();const first=request.keyFor('{"rate":10}');
  expect(request.keyFor('{"rate":10}')).toBe(first);expect(request.keyFor('{"rate":20}')).not.toBe(first);
 });
});
